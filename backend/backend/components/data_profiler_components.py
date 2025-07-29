# backend/backend/components/data_profiler_components.py
import reflex as rx
from typing import Dict, Any, List


def table_selector() -> rx.Component:
    """Компонент выбора таблицы для профилирования"""
    from ..pages.data_profiler import DataProfilerState

    return rx.box(
        rx.heading("Выберите таблицу", size="5", margin_bottom="15px", color="white"),

        rx.cond(
            DataProfilerState.tables_list.length() > 0,
            rx.grid(
                rx.foreach(
                    DataProfilerState.tables_list,
                    lambda table: rx.box(
                        rx.vstack(
                            rx.hstack(
                                rx.icon("database", size=20, color="blue.400"),
                                rx.text(
                                    f"{table['database']}.{table['table_name']}",
                                    font_weight="bold",
                                    color="white"
                                ),
                                spacing="2"
                            ),
                            rx.hstack(
                                rx.text(
                                    f"Строк: {format_number(table['total_rows'])}",
                                    color="gray.400",
                                    font_size="sm"
                                ),
                                rx.text("•", color="gray.600"),
                                rx.text(
                                    f"Размер: {table['size_readable']}",
                                    color="gray.400",
                                    font_size="sm"
                                ),
                                spacing="1"
                            ),
                            spacing="1",
                            align="start"
                        ),
                        padding="15px",
                        background_color=rx.cond(
                            (DataProfilerState.selected_database == table['database']) &
                            (DataProfilerState.selected_table == table['table_name']),
                            "blue.800",
                            "gray.700"
                        ),
                        border_radius="8px",
                        cursor="pointer",
                        _hover={"background_color": "gray.600"},
                        on_click=lambda: DataProfilerState.select_table(
                            table['database'],
                            table['table_name']
                        ),
                        transition="all 0.2s"
                    )
                ),
                columns="3",
                spacing="3",
                width="100%"
            ),
            rx.text("Загрузка списка таблиц...", color="gray.400")
        ),

        background_color="gray.800",
        padding="20px",
        border_radius="10px",
        width="100%"
    )


def profile_overview() -> rx.Component:
    """Общая информация о профилированной таблице"""
    from ..pages.data_profiler import DataProfilerState

    general_stats = DataProfilerState.profile_results.get('general_stats', {})

    return rx.box(
        rx.heading("Общая информация", size="5", margin_bottom="15px", color="white"),

        rx.grid(
            # Количество строк
            rx.box(
                rx.vstack(
                    rx.icon("rows", size=24, color="blue.400"),
                    rx.text("Строк", color="gray.400", font_size="sm"),
                    rx.text(
                        format_number(general_stats.get('row_count', 0)),
                        font_size="2xl",
                        font_weight="bold",
                        color="white"
                    ),
                    spacing="1",
                    align="center"
                ),
                padding="15px",
                background_color="gray.700",
                border_radius="8px"
            ),

            # Количество колонок
            rx.box(
                rx.vstack(
                    rx.icon("columns", size=24, color="green.400"),
                    rx.text("Колонок", color="gray.400", font_size="sm"),
                    rx.text(
                        str(general_stats.get('column_count', 0)),
                        font_size="2xl",
                        font_weight="bold",
                        color="white"
                    ),
                    spacing="1",
                    align="center"
                ),
                padding="15px",
                background_color="gray.700",
                border_radius="8px"
            ),

            # Размер таблицы
            rx.box(
                rx.vstack(
                    rx.icon("hard_drive", size=24, color="purple.400"),
                    rx.text("Размер", color="gray.400", font_size="sm"),
                    rx.text(
                        general_stats.get('size_readable', '0 B'),
                        font_size="2xl",
                        font_weight="bold",
                        color="white"
                    ),
                    spacing="1",
                    align="center"
                ),
                padding="15px",
                background_color="gray.700",
                border_radius="8px"
            ),

            # Время профилирования
            rx.box(
                rx.vstack(
                    rx.icon("clock", size=24, color="orange.400"),
                    rx.text("Профилировано", color="gray.400", font_size="sm"),
                    rx.text(
                        format_timestamp(DataProfilerState.profile_results.get('profiled_at', '')),
                        font_size="md",
                        color="white"
                    ),
                    spacing="1",
                    align="center"
                ),
                padding="15px",
                background_color="gray.700",
                border_radius="8px"
            ),

            columns=4,
            spacing=3,
            width="100%"
        ),

        background_color="gray.800",
        padding="20px",
        border_radius="10px",
        width="100%"
    )


def column_statistics() -> rx.Component:
    """Таблица со статистикой по колонкам"""
    from ..pages.data_profiler import DataProfilerState

    return rx.scroll_area(
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Колонка", color="gray.300"),
                    rx.table.column_header_cell("Тип", color="gray.300"),
                    rx.table.column_header_cell("NULL %", color="gray.300"),
                    rx.table.column_header_cell("Уникальных", color="gray.300"),
                    rx.table.column_header_cell("Мин", color="gray.300"),
                    rx.table.column_header_cell("Макс", color="gray.300"),
                    rx.table.column_header_cell("Среднее", color="gray.300"),
                    rx.table.column_header_cell("Паттерны", color="gray.300"),
                )
            ),
            rx.table.body(
                rx.foreach(
                    DataProfilerState.profile_results.get('column_stats', []),
                    lambda col: rx.table.row(
                        # Название колонки (кликабельное)
                        rx.table.cell(
                            rx.text(
                                col['column_name'],
                                color="blue.400",
                                cursor="pointer",
                                _hover={"color": "blue.300"},
                                on_click=lambda: DataProfilerState.select_column(col['column_name'])
                            )
                        ),

                        # Тип данных
                        rx.table.cell(
                            rx.hstack(
                                get_type_icon(col.get('inferred_type', 'other')),
                                rx.text(
                                    col.get('data_type', '').split('(')[0],
                                    color="gray.400",
                                    font_size="sm"
                                ),
                                spacing="1"
                            )
                        ),

                        # NULL процент
                        rx.table.cell(
                            rx.hstack(
                                rx.text(
                                    f"{col.get('null_percentage', 0)}%",
                                    color=get_null_color(col.get('null_percentage', 0))
                                ),
                                rx.cond(
                                    col.get('null_percentage', 0) > 50,
                                    rx.icon("alert_triangle", size=14, color="orange.400")
                                ),
                                spacing="1"
                            )
                        ),

                        # Уникальных значений
                        rx.table.cell(
                            rx.vstack(
                                rx.text(
                                    format_number(col.get('unique_count', 0)),
                                    color="white"
                                ),
                                rx.text(
                                    f"{col.get('unique_percentage', 0)}%",
                                    color="gray.500",
                                    font_size="xs"
                                ),
                                spacing="0"
                            )
                        ),

                        # Минимум (для числовых)
                        rx.table.cell(
                            rx.text(
                                format_value(col.get('min')),
                                color="gray.300"
                            )
                        ),

                        # Максимум (для числовых)
                        rx.table.cell(
                            rx.text(
                                format_value(col.get('max')),
                                color="gray.300"
                            )
                        ),

                        # Среднее (для числовых)
                        rx.table.cell(
                            rx.text(
                                format_value(col.get('mean')),
                                color="gray.300"
                            )
                        ),

                        # Паттерны (для строк)
                        rx.table.cell(
                            rx.hstack(
                                rx.foreach(
                                    list(col.get('patterns', {}).keys()),
                                    lambda pattern: rx.badge(
                                        pattern,
                                        color_scheme=get_pattern_color(pattern),
                                        size="sm"
                                    )
                                ),
                                spacing="1"
                            )
                        ),

                        _hover={"background_color": "gray.700"}
                    )
                )
            ),
            width="100%",
            variant="surface"
        ),
        height="400px",
        width="100%"
    )


def distribution_chart() -> rx.Component:
    """График распределения значений колонки"""
    from ..pages.data_profiler import DataProfilerState

    distribution = DataProfilerState.column_distribution

    return rx.cond(
        distribution != {},
        rx.cond(
            distribution.get('type') == 'numeric',
            # Гистограмма для числовых данных
            rx.recharts.bar_chart(
                rx.recharts.bar(
                    data_key="count",
                    fill="#3B82F6"
                ),
                rx.recharts.x_axis(
                    data_key="bin",
                    angle=-45,
                    text_anchor="end",
                    height=80
                ),
                rx.recharts.y_axis(),
                rx.recharts.tooltip(),
                data=[
                    {"bin": bin_label, "count": count}
                    for bin_label, count in zip(
                        distribution.get('bins', []),
                        distribution.get('counts', [])
                    )
                ],
                height=300,
                width="100%"
            ),

            # Столбчатая диаграмма для категориальных данных
            rx.recharts.bar_chart(
                rx.recharts.bar(
                    data_key="count",
                    fill="#10B981"
                ),
                rx.recharts.x_axis(
                    data_key="value",
                    angle=-45,
                    text_anchor="end",
                    height=100
                ),
                rx.recharts.y_axis(),
                rx.recharts.tooltip(),
                data=[
                    {"value": value[:20] + "..." if len(value) > 20 else value, "count": count}
                    for value, count in zip(
                        distribution.get('values', []),
                        distribution.get('counts', [])
                    )
                ],
                height=300,
                width="100%"
            )
        ),
        rx.text("Выберите колонку для визуализации", color="gray.400")
    )


def top_values_table() -> rx.Component:
    """Таблица с топ значениями колонки"""
    from ..pages.data_profiler import DataProfilerState

    # Находим статистику выбранной колонки
    column_stats = None
    for col in DataProfilerState.profile_results.get('column_stats', []):
        if col['column_name'] == DataProfilerState.selected_column:
            column_stats = col
            break

    if not column_stats:
        return rx.text("Нет данных", color="gray.400")

    top_values = column_stats.get('top_values', [])

    return rx.scroll_area(
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Значение", color="gray.300"),
                    rx.table.column_header_cell("Количество", color="gray.300"),
                    rx.table.column_header_cell("%", color="gray.300"),
                )
            ),
            rx.table.body(
                rx.foreach(
                    top_values,
                    lambda val: rx.table.row(
                        rx.table.cell(
                            rx.text(
                                truncate_string(str(val['value']), 50),
                                color="white",
                                title=str(val['value'])
                            )
                        ),
                        rx.table.cell(
                            rx.text(
                                format_number(val['count']),
                                color="gray.300"
                            )
                        ),
                        rx.table.cell(
                            rx.progress(
                                value=val['percentage'],
                                width="80px",
                                height="20px",
                                color_scheme="blue"
                            ),
                            rx.text(
                                f"{val['percentage']}%",
                                color="gray.400",
                                font_size="sm",
                                margin_left="10px"
                            )
                        ),
                        _hover={"background_color": "gray.700"}
                    )
                )
            ),
            width="100%",
            variant="surface"
        ),
        height="280px",
        width="100%"
    )


# Вспомогательные функции
def format_number(num: Any) -> str:
    """Форматирование чисел с разделителями тысяч"""
    if num is None:
        return "—"
    try:
        if isinstance(num, (int, float)):
            return f"{int(num):,}".replace(",", " ")
        return str(num)
    except:
        return str(num)


def format_value(val: Any) -> str:
    """Форматирование значения для отображения"""
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def format_timestamp(timestamp: str) -> str:
    """Форматирование временной метки"""
    if not timestamp:
        return "—"
    try:
        # Простое форматирование ISO timestamp
        return timestamp.split('.')[0].replace('T', ' ')
    except:
        return timestamp


def truncate_string(s: str, max_length: int) -> str:
    """Обрезка длинных строк"""
    if len(s) <= max_length:
        return s
    return s[:max_length] + "..."


def get_type_icon(inferred_type: str) -> rx.Component:
    """Иконка для типа данных"""
    icons = {
        'numeric': rx.icon("hash", size=16, color="blue.400"),
        'string': rx.icon("text", size=16, color="green.400"),
        'date': rx.icon("calendar", size=16, color="purple.400"),
        'boolean': rx.icon("toggle_left", size=16, color="orange.400"),
        'array': rx.icon("list", size=16, color="pink.400"),
        'other': rx.icon("help_circle", size=16, color="gray.400")
    }
    return icons.get(inferred_type, icons['other'])


def get_null_color(percentage: float) -> str:
    """Цвет для процента NULL значений"""
    if percentage == 0:
        return "green.400"
    elif percentage < 10:
        return "yellow.400"
    elif percentage < 50:
        return "orange.400"
    else:
        return "red.400"


def get_pattern_color(pattern: str) -> str:
    """Цветовая схема для паттернов"""
    colors = {
        'email': 'blue',
        'phone': 'green',
        'url': 'purple',
        'numeric': 'orange',
        'alphanumeric': 'teal',
        'uuid': 'pink'
    }
    return colors.get(pattern, 'gray')