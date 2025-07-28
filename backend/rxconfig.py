import reflex as rx
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

config = rx.Config(
    app_name="backend",
    frontend_port=3000,
    backend_port=8000,
    # Отключаем sitemap плагин чтобы убрать предупреждение
    disable_plugins=["reflex.plugins.sitemap.SitemapPlugin"],
)