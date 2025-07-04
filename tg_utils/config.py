import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

ADMIN_IDS = [int(id) for id in (os.getenv('ADMIN_IDS', '')).split(',') if id]
AUTHORIZED_TELEGRAM_IDS = [int(id) for id in (os.getenv('AUTHORIZED_TELEGRAM_IDS', '')).split(',') if id]