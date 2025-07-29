# backend/backend/pages/data_profiler.py
import reflex as rx
from typing import List, Dict, Any
from ..services.data_profiler_service import DataProfilerService
from ..components.data_profiler_components import (
    table_selector,
    profile_overview,
    column_statistics,
    distribution_chart,
    top_values_table
)

# Инициализация сервиса
profiler_service = DataProfilerService()


class DataProfilerState(rx.State):
    """Состояние страницы Data Profiler"""

    # Список таблиц
    tables_list: List[Dict[str, str]] = []
    selected_database: str = ""
    selected_table: str = ""

    # Результаты профилирования
    profile_results: Dict[str, Any] = {}
    is_loading: bool = False
    error_message: str = ""

    # Выбранная колонка для детального анализа
    selected_column: str = ""
    column_distribution: Dict[str, Any] = {}

    # Параметры профилирования
    sample_size: int = 10000

    def load_tables(self):
        """Загрузить список доступных таблиц"""
        self.tables_list = profiler_service.get_tables_list()
        if self.tables_list:
            # Автоматически выбираем первую таблицу
            first_table = self.tables_list[0]
            self.selected_database = first_table['database']
            self.selected_table = first_table['table_name']

    def select_table(self, database: str, table_name: str):
        """Выбрать таблицу для профилирования"""
        self.selected_database = database
        self.selected_table = table_name
        self.profile_results = {}
        self.selected_column = ""
        self.column_distribution = {}

    def run_profiling(self):
        """Запустить профилирование выбранной таблицы"""
        if not self.selected_database or not self.selected_table:
            self.error_message = "Пожалуйста, выберите таблицу для анализа"
            return

        self.is_loading = True
        self.error_message = ""

        try:
            self.profile_results = profiler_service.profile_table(
                self.selected_database,
                self.selected_table,
                self.sample_size
            )

            if 'error' in self.profile_results:
                self.error_message = f"Ошибка профилирования: {self.profile_results['error']}"
            else:
                # Автоматически выбираем первую колонку для визуализации
                if self.profile_results.get('column_stats'):
                    first_column = self.profile_results['column_stats'][0]['column_name']
                    self.select_column(first_column)
        except Exception as e:
            self.error_message = f"Ошибка: {str(e)}"
        finally:
            self.is_loading = False

    def select_column(self, column_name: str):
        """Выбрать колонку для детального анализа"""
        self.selected_column = column_name

        # Загружаем распределение для выбранной колонки
        if self.selected_database and self.selected_table:
            self.column_distribution = profiler_service.get_column_distribution(
                self.selected_database,
                self.selected_table,
                column_name,
                bins=20
            )

    def update_sample_size(self, value: str):
        """Обновить размер выборки"""
        try:
            self.sample_size = int(value)
        except:
            self.sample_size = 10000

    def export_profile(self):
        """Экспорт результатов профилирования"""
        # TODO: Реализовать экспорт в Excel/JSON
        pass


def data_profiler_page() -> rx.Component:
    """Страница Data Profiler"""
    return rx.box(
        # Заголовок
        rx.heading(
            "Data Profiler",
            size="4",
            margin_bottom="20px",
            color="white"
        ),

        rx.text(
            "Быстрый анализ и профилирование любой таблицы или датасета",
            color="gray.400",
            margin_bottom="30px"
        ),

        # Ошибки
        rx.cond(
            DataProfilerState.error_message != "",
            rx.box(
                rx.hstack(
                    rx.icon("circle_alert", color="red.500"),
                    rx.text(DataProfilerState.error_message, color="red.500"),
                    spacing="2"
                ),
                padding="15px",
                background_color="red.100",
                border_radius="8px",
                margin_bottom="20px"
            )
        ),

        # Основной контент
        rx.vstack(
            # Селектор таблицы
            table_selector(),

            # Кнопка запуска профилирования
            rx.hstack(
                rx.button(
                    rx.cond(
                        DataProfilerState.is_loading,
                        rx.spinner(size="sm", color="white"),
                        rx.text("Запустить анализ")
                    ),
                    on_click=DataProfilerState.run_profiling,
                    background_color="blue.600",
                    _hover={"background_color": "blue.700"},
                    is_disabled=DataProfilerState.is_loading,
                    width="200px"
                ),

                rx.hstack(
                    rx.text("Размер выборки:", color="gray.400"),
                    rx.number_input(
                        value=DataProfilerState.sample_size,
                        on_change=DataProfilerState.update_sample_size,
                        width="120px",
                        min=1000,
                        max=1000000,
                        step=1000
                    ),
                    spacing="2"
                ),

                spacing="4",
                align="center"
            ),

            # Результаты профилирования
            rx.cond(
                DataProfilerState.profile_results != {},
                rx.vstack(
                    # Общая информация о таблице
                    profile_overview(),

                    # Статистика по колонкам
                    rx.box(
                        rx.heading("Статистика по колонкам", size="5", margin_bottom="15px", color="white"),
                        column_statistics(),
                        background_color="gray.800",
                        padding="20px",
                        border_radius="10px",
                        width="100%"
                    ),

                    # Визуализация распределения
                    rx.cond(
                        DataProfilerState.selected_column != "",
                        rx.hstack(
                            # График распределения
                            rx.box(
                                rx.heading(
                                    rx.text.span("Распределение: ", color="white"),
                                    rx.text.span(DataProfilerState.selected_column, color="blue.400"),
                                    size="5",
                                    margin_bottom="15px"
                                ),
                                distribution_chart(),
                                background_color="gray.800",
                                padding="20px",
                                border_radius="10px",
                                width="60%"
                            ),

                            # Топ значения
                            rx.box(
                                rx.heading("Топ значений", size="5", margin_bottom="15px", color="white"),
                                top_values_table(),
                                background_color="gray.800",
                                padding="20px",
                                border_radius="10px",
                                width="40%"
                            ),

                            spacing="4",
                            width="100%"
                        )
                    ),

                    spacing="4",
                    width="100%"
                )
            ),

            spacing="4",
            width="100%"
        ),

        padding="20px",
        on_mount=DataProfilerState.load_tables
    )