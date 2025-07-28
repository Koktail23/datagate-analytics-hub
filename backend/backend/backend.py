import reflex as rx
from .pages.validator import validator_page

# Состояние приложения
class State(rx.State):
    """Базовое состояние приложения."""
    theme: str = "light"

    def toggle_theme(self):
        self.theme = "dark" if self.theme == "light" else "light"


# Компонент заголовка
def header() -> rx.Component:
    return rx.box(
        rx.hstack(
            rx.heading("DataGate Analytics Hub", size="7"),
            rx.spacer(),
            rx.button(
                rx.cond(
                    State.theme == "light",
                    rx.icon("moon"),
                    rx.icon("sun"),
                ),
                on_click=State.toggle_theme,
                variant="ghost",
                size="3",
            ),
            width="100%",
            padding="1rem",
        ),
        background="var(--gray-2)",
        border_bottom="1px solid var(--gray-4)",
    )


# Главная страница
def index() -> rx.Component:
    return rx.vstack(
        header(),
        rx.center(
            rx.vstack(
                rx.heading("Добро пожаловать!", size="9"),
                rx.text("Интегрированная аналитическая платформа", size="5"),
                rx.hstack(
                    rx.button(
                        "Валидатор данных",
                        size="4",
                        color_scheme="blue",
                        radius="full",
                    ),
                    rx.button(
                        "KPI Dashboard",
                        size="4",
                        color_scheme="green",
                        radius="full",
                    ),
                    rx.button(
                        "Data Profiler",
                        size="4",
                        color_scheme="purple",
                        radius="full",
                    ),
                    spacing="4",
                ),
                spacing="5",
                align="center",
            ),
            height="80vh",
        ),
        width="100%",
        height="100vh",
        spacing="0",
    )


# Создаем приложение
app = rx.App()
app.add_page(index, route="/", title="DataGate Analytics Hub")
app.add_page(validator_page, route="/validator", title="Валидатор данных - DataGate")