import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, 'storage', 'plugins')
DB_PATH = os.path.join(DB_DIR, 'steam_rental.db')

# Убедимся, что директория существует
os.makedirs(DB_DIR, exist_ok=True)

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN")
if not TG_TOKEN:
    raise ValueError("Токен Telegram бота (TG_TOKEN) не найден в файле .env")


# ID администраторов Telegram (пример)
ADMIN_IDS = [618337960]

# Авторизованные Telegram ID для доступа к боту
AUTHORIZED_TELEGRAM_IDS = [618337960] 