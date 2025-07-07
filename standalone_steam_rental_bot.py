# Импортируем необходимые модули
import telebot
from telebot import apihelper
import os
from dotenv import load_dotenv
import logging
import sys
import pytz
import time
from tg_utils.config import ADMIN_IDS, AUTHORIZED_TELEGRAM_IDS
from tg_utils.handlers import init_handlers
from tg_utils.db import init_db, ensure_accounts_columns, restore_rental_timers, DB_PATH
from tg_utils.logger import logger, LOG_STYLES
from colorama import Fore, Style, init

# Инициализация colorama
init(autoreset=True)

# STEAM RENTAL Logo
logo = f"""
{Fore.CYAN}{Style.BRIGHT}███████╗████████╗███████╗ █████╗ ███╗   ███╗
██╔════╝╚══██╔══╝██╔════╝██╔══██╗████╗ ████║
███████╗   ██║   █████╗  ███████║██╔████╔██║
╚════██║   ██║   ██╔══╝  ██╔══██║██║╚██╔╝██║
███████║   ██║   ███████╗██║  ██║██║ ╚═╝ ██║
╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝{Style.RESET_ALL}
                                             
        {Fore.RED}{Style.BRIGHT}██████╗ ███████╗███╗   ██╗████████╗ █████╗ ██╗     
        ██╔══██╗██╔════╝████╗  ██║╚══██╔══╝██╔══██╗██║     
        ██████╔╝█████╗  ██╔██╗ ██║   ██║   ███████║██║     
        ██╔══██╗██╔══╝  ██║╚██╗██║   ██║   ██╔══██║██║     
        ██║  ██║███████╗██║ ╚████║   ██║   ██║  ██║███████╗
        ╚═╝  ╚═╝╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚═╝  ╚═╝╚══════╝{Style.RESET_ALL}
"""

VERSION = "2.0.0"

# Включаем middleware
apihelper.ENABLE_MIDDLEWARE = True

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Загружаем переменные окружения
load_dotenv()

# Проверяем токен
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN")
if not TG_TOKEN:
    raise ValueError("Токен Telegram бота не найден в переменных окружения")

# Инициализируем бота
bot = telebot.TeleBot(TG_TOKEN)

def is_user_authorized(user_id):
    return user_id in AUTHORIZED_TELEGRAM_IDS

def auth_required(func):
    def wrapper(*args, **kwargs):
        user_id = None
        if args and len(args) > 0:
            if hasattr(args[0], 'chat'):
                user_id = args[0].chat.id
            elif hasattr(args[0], 'message'):
                user_id = args[0].message.chat.id
        if user_id is None:
            return
        if not is_user_authorized(user_id):
            try:
                chat_id = user_id
                bot.send_message(chat_id, "⛔ Доступ запрещен. У вас нет прав для использования этого бота.")
                if hasattr(args[0], 'id'):
                    bot.answer_callback_query(args[0].id, "Доступ запрещен!", show_alert=True)
            except Exception:
                pass
            return
        return func(*args, **kwargs)
    return wrapper

def main():
    # Отображаем логотип STEAM RENTAL
    print(logo)
    print(f"{Fore.RED}{Style.BRIGHT}v{VERSION}{Style.RESET_ALL}\n")
    print(f"{Fore.MAGENTA}{Style.BRIGHT}By Promble{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{Style.BRIGHT} * GitHub: {Fore.BLUE}{Style.BRIGHT}https://github.com/Promble/SteamRentalBot{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{Style.BRIGHT} * Telegram: {Fore.BLUE}{Style.BRIGHT}https://t.me/Promble{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{Style.BRIGHT} * Discord: {Fore.BLUE}{Style.BRIGHT}https://discord.gg/yourserver\n{Style.RESET_ALL}")
    time.sleep(1)
    
    # Инициализация базы данных
    init_db()
    ensure_accounts_columns()
    restore_rental_timers()
    logger.info("🧐 База данных и таймеры успешно инициализированы")
    
    # FunPayListener
    try:
        from funpay_integration import FunPayListener
        fp_listener = FunPayListener()
        fp_listener.start()
        logger.info("O_O FunPayListener успешно запущен. Ожидание заказов с FunPay...")
    except Exception as e:
        logger.warning(f"😭 FunPay интеграция не активна: {e}")
    
    # Инициализация обработчиков
    init_handlers(bot, is_user_authorized, auth_required, admin_ids=ADMIN_IDS)
    
    # Запуск бота с обработкой ошибок
    while True:
        try:
            bot.polling(none_stop=True, interval=0)
            logger.warning("☠️ Polling завершился без ошибки. Перезапуск через 5 секунд...")
            time.sleep(5)
            logger.info("✅ Бот успешно перезапущен и готов к работе! ✅")
            
        except Exception as e:
            logger.error(f"☠️🏩 Ошибка в polling: {e}")
            for admin_id in ADMIN_IDS:
                try:
                    error_message = f"☠️ Бот перезапускается из-за ошибки: {e}\n\nПроверьте логи для получения дополнительной информации."
                    bot.send_message(admin_id, error_message)
                except Exception as send_e:
                    logger.error(f"Не удалось отправить уведомление об ошибке администратору {admin_id}: {send_e}")
            logger.info("😘 Попытка перезапуска через 5 секунд...")
            time.sleep(5)

if __name__ == "__main__":
    main()