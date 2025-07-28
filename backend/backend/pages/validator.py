import reflex as rx
from ..components.navbar import navbar
from ..services.clickhouse_service import clickhouse_service
import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import os
import io


class ValidatorState(rx.State):
    """Состояние страницы валидатора."""
    upload_progress: int = 0
    is_validating: bool = False
    validation_results: List[Dict[str, Any]] = []
    selected_file: str = ""
    error_message: str = ""
    success_message: str = ""

    # Статистика
    total_checks: int = 0
    failed_checks: int = 0
    success_rate: float = 0.0

    async def handle_upload(self, files: List[rx.UploadFile]):
        """Обработка загруженного файла."""
        if not files:
            return

        # Создаем папку если не существует
        os.makedirs("data/uploads", exist_ok=True)

        for file in files:
            # Получаем имя файла правильным способом
            filename = file.name if hasattr(file, 'name') else 'uploaded_file.csv'
            self.selected_file = filename
            self.error_message = ""
            self.success_message = ""

            try:
                # Читаем содержимое файла
                upload_data = await file.read()

                # Начинаем валидацию
                self.is_validating = True
                yield

                # Читаем данные в зависимости от типа файла
                if filename.endswith('.csv'):
                    df = pd.read_csv(io.BytesIO(upload_data))
                elif filename.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(io.BytesIO(upload_data))
                else:
                    self.error_message = "Поддерживаются только CSV и Excel файлы"
                    self.is_validating = False
                    return

                # Сохраняем файл для истории
                file_path = f"data/uploads/{filename}"
                with open(file_path, "wb") as f:
                    f.write(upload_data)

                # Выполняем проверки
                results = self.validate_dataframe(df, filename)
                self.validation_results = results

                # Обновляем статистику
                self.total_checks = len(results)
                self.failed_checks = len([r for r in results if r['check_status'] == 'FAILED'])
                self.success_rate = ((
                                                 self.total_checks - self.failed_checks) / self.total_checks * 100) if self.total_checks > 0 else 0

                self.success_message = f"Валидация завершена! Выполнено {self.total_checks} проверок."

            except Exception as e:
                self.error_message = f"Ошибка при обработке файла: {str(e)}"
                print(f"Upload error: {str(e)}")  # Для отладки
            finally:
                self.is_validating = False
                yield

    def validate_dataframe(self, df: pd.DataFrame, filename: str) -> List[Dict[str, Any]]:
        """Выполнить валидацию DataFrame."""
        results = []

        # Проверка на пустые значения
        for column in df.columns:
            null_count = df[column].isnull().sum()
            total_count = len(df)
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

            check_data = {
                'dataset_name': filename,
                'table_name': 'uploaded_data',
                'column_name': str(column),
                'check_type': 'NULL_CHECK',
                'check_status': 'PASSED' if null_percentage < 10 else 'FAILED',
                'error_count': int(null_count),
                'total_count': total_count,
                'error_percentage': float(null_percentage),
                'details': f'{null_count} null values found ({null_percentage:.2f}%)'
            }

            results.append(check_data)
            try:
                clickhouse_service.insert_quality_check(check_data)
            except Exception as e:
                print(f"Error inserting to ClickHouse: {e}")

        # Проверка на дубликаты
        duplicate_count = df.duplicated().sum()
        duplicate_percentage = (duplicate_count / len(df) * 100) if len(df) > 0 else 0

        check_data = {
            'dataset_name': filename,
            'table_name': 'uploaded_data',
            'column_name': 'ALL_COLUMNS',
            'check_type': 'DUPLICATE_CHECK',
            'check_status': 'PASSED' if duplicate_percentage < 5 else 'FAILED',
            'error_count': int(duplicate_count),
            'total_count': len(df),
            'error_percentage': float(duplicate_percentage),
            'details': f'{duplicate_count} duplicate rows found ({duplicate_percentage:.2f}%)'
        }

        results.append(check_data)
        try:
            clickhouse_service.insert_quality_check(check_data)
        except Exception as e:
            print(f"Error inserting to ClickHouse: {e}")

        # Проверка типов данных
        for column in df.columns:
            if df[column].dtype == 'object':
                # Проверяем, можно ли конвертировать в числа
                try:
                    pd.to_numeric(df[column], errors='coerce')
                    non_numeric_count = pd.to_numeric(df[column], errors='coerce').isna().sum()
                    if non_numeric_count > 0 and non_numeric_count < len(df):
                        check_data = {
                            'dataset_name': filename,
                            'table_name': 'uploaded_data',
                            'column_name': str(column),
                            'check_type': 'DATA_TYPE_CHECK',
                            'check_status': 'WARNING',
                            'error_count': int(non_numeric_count),
                            'total_count': len(df),
                            'error_percentage': float(non_numeric_count / len(df) * 100),
                            'details': f'Mixed data types detected: {non_numeric_count} non-numeric values in potentially numeric column'
                        }
                        results.append(check_data)
                except:
                    pass

        return results


def validator_page() -> rx.Component:
    """Страница валидатора данных."""
    return rx.vstack(
        navbar(),
        rx.container(
            rx.vstack(
                rx.heading("Валидатор качества данных", size="8"),
                rx.text("Автоматическая проверка данных на аномалии и ошибки", size="4", color="gray"),

                # Область загрузки файла
                rx.card(
                    rx.vstack(
                        rx.heading("Загрузка данных", size="5"),
                        rx.upload(
                            rx.vstack(
                                rx.icon("upload", size=48),
                                rx.text("Перетащите файл сюда или нажмите для выбора"),
                                rx.text("Поддерживаются CSV и Excel файлы", size="2", color="gray"),
                            ),
                            id="upload1",
                            accept=[".csv", ".xlsx", ".xls"],
                            max_files=1,
                        ),
                        rx.button(
                            "Загрузить файл",
                            on_click=lambda: ValidatorState.handle_upload(
                                rx.upload_files(upload_id="upload1")
                            ),
                            color_scheme="blue",
                            width="100%",
                        ),
                        rx.cond(
                            ValidatorState.selected_file != "",
                            rx.text(f"Выбран файл: {ValidatorState.selected_file}", color="blue"),
                        ),
                        spacing="4",
                        width="100%",
                    ),
                    width="100%",
                ),

                # Сообщения об ошибках/успехе
                rx.cond(
                    ValidatorState.error_message != "",
                    rx.callout(
                        ValidatorState.error_message,
                        icon="circle_alert",  # Исправленная иконка
                        color_scheme="red",
                    ),
                ),
                rx.cond(
                    ValidatorState.success_message != "",
                    rx.callout(
                        ValidatorState.success_message,
                        icon="circle_check",  # Исправленная иконка
                        color_scheme="green",
                    ),
                ),

                # Индикатор загрузки
                rx.cond(
                    ValidatorState.is_validating,
                    rx.vstack(
                        rx.spinner(size="3"),
                        rx.text("Выполняется валидация данных..."),
                        align="center",
                        width="100%",
                    ),
                ),

                # Статистика
                rx.cond(
                    ValidatorState.total_checks > 0,
                    rx.grid(
                        rx.card(
                            rx.vstack(
                                rx.text("Всего проверок", size="2", weight="bold"),
                                rx.heading(ValidatorState.total_checks, size="8"),
                            ),
                        ),
                        rx.card(
                            rx.vstack(
                                rx.text("Провалено", size="2", weight="bold"),
                                rx.heading(ValidatorState.failed_checks, size="8", color="red"),
                            ),
                        ),
                        rx.card(
                            rx.vstack(
                                rx.text("Успешность", size="2", weight="bold"),
                                rx.heading(f"{ValidatorState.success_rate:.1f}%", size="8", color="green"),
                            ),
                        ),
                        columns="3",
                        spacing="4",
                        width="100%",
                    ),
                ),

                # Результаты валидации
                rx.cond(
                    ValidatorState.total_checks > 0,
                    rx.card(
                        rx.vstack(
                            rx.heading("Результаты проверок", size="5"),
                            rx.foreach(
                                ValidatorState.validation_results,
                                lambda result: rx.box(
                                    rx.hstack(
                                        rx.cond(
                                            result['check_status'] == 'PASSED',
                                            rx.icon("circle_check", color="green"),  # Исправленная иконка
                                            rx.cond(
                                                result['check_status'] == 'WARNING',
                                                rx.icon("triangle_alert", color="orange"),  # Исправленная иконка
                                                rx.icon("circle_x", color="red"),  # Исправленная иконка
                                            ),
                                        ),
                                        rx.vstack(
                                            rx.text(
                                                f"{result['check_type']} - {result['column_name']}",
                                                weight="bold",
                                            ),
                                            rx.text(result['details'], size="2", color="gray"),
                                            spacing="1",
                                        ),
                                        rx.spacer(),
                                        rx.badge(
                                            result['check_status'],
                                            color_scheme=rx.cond(
                                                result['check_status'] == 'PASSED',
                                                "green",
                                                rx.cond(
                                                    result['check_status'] == 'WARNING',
                                                    "orange",
                                                    "red"
                                                )
                                            ),
                                        ),
                                        width="100%",
                                        padding="1rem",
                                        align="center",
                                    ),
                                    border_bottom="1px solid var(--gray-4)",
                                ),
                            ),
                            width="100%",
                        ),
                        width="100%",
                    ),
                ),

                spacing="6",
                width="100%",
                max_width="1200px",
                margin="0 auto",
                padding="2rem",
            ),
        ),
        width="100%",
    )