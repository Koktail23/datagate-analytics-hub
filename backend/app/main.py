import reflex as rx
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация приложения
config = rx.Config(
    app_name="datagate_analytics",
    api_url="http://localhost:8000",
    db_url="sqlite:///reflex.db",
)

# Главная страница
def index():
    return rx.center(
        rx.vstack(
            rx.heading("DataGate Analytics Hub", size="9"),
            rx.text("Интегрированная аналитическая платформа"),
            rx.button(
                "Начать работу",
                size="4",
                color_scheme="blue",
            ),
            spacing="5",
        ),
        height="100vh",
    )

# Создаем приложение
app = rx.App()
app.add_page(index, route="/")