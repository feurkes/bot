#!/usr/bin/env python3
"""
Steam Rental Bot - Главный файл для запуска бота
Простой и понятный запуск бота с цветным логированием
"""

import os
import sys
import time
from dotenv import load_dotenv

# Импорт цветного логирования
from tg_utils.logger import logger, log_bright, log_success, log_error, log_warning, log_info
from colorama import Fore, Style

def check_environment():
    """Проверка переменных окружения"""
    log_info("Проверка переменных окружения...")
    
    # Загружаем переменные окружения
    load_dotenv()
    
    # Проверяем токен
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN")
    if not token:
        log_error("Токен Telegram бота не найден!")
        log_warning("Запустите first_setup.py для первоначальной настройки")
        return None
    
    log_success("Токен бота найден")
    return token

def initialize_bot():
    """Инициализация бота и компонентов"""
    log_bright("🚀 Инициализация Steam Rental Bot", Fore.MAGENTA)
    
    # Проверяем окружение
    token = check_environment()
    if not token:
        return None, None, None, None
    
    try:
        # Импорт после проверки окружения
        import telebot
        from telebot import apihelper
        from tg_utils.config import ADMIN_IDS, AUTHORIZED_TELEGRAM_IDS
        from tg_utils.handlers import init_handlers
        
        # Включаем middleware
        apihelper.ENABLE_MIDDLEWARE = True
        
        # Создаем бота
        bot = telebot.TeleBot(token)
        log_success("Бот успешно инициализирован")
        
        # Функции авторизации
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
        
        return bot, is_user_authorized, auth_required, ADMIN_IDS
        
    except ImportError as e:
        log_error(f"Ошибка импорта модулей: {e}")
        log_warning("Возможно, нужно запустить install_requirements.py")
        return None, None, None, None
    except Exception as e:
        log_error(f"Ошибка инициализации бота: {e}")
        return None, None, None, None

def initialize_database():
    """Инициализация базы данных"""
    log_info("Инициализация базы данных...")
    
    try:
        from tg_utils.db import init_db, ensure_accounts_columns, restore_rental_timers
        
        init_db()
        ensure_accounts_columns()
        restore_rental_timers()
        log_success("База данных и таймеры успешно инициализированы")
        return True
        
    except Exception as e:
        log_error(f"Ошибка инициализации БД: {e}")
        return False

def initialize_funpay():
    """Инициализация FunPay интеграции"""
    log_info("Инициализация FunPay интеграции...")
    
    try:
        from funpay_integration import FunPayListener
        fp_listener = FunPayListener()
        fp_listener.start()
        log_success("FunPayListener успешно запущен. Ожидание заказов с FunPay...")
        return True
        
    except Exception as e:
        log_warning(f"FunPay интеграция не активна: {e}")
        return False

def run_bot_polling(bot, admin_ids):
    """Запуск polling бота с обработкой ошибок"""
    log_bright("🤖 Запуск бота...", Fore.GREEN)
    
    while True:
        try:
            bot.polling(none_stop=True, interval=0)
            log_warning("Polling завершился без ошибки. Перезапуск через 5 секунд...")
            time.sleep(5)
            log_success("Бот успешно перезапущен и готов к работе!")
            
        except Exception as e:
            log_error(f"Ошибка в polling: {e}")
            
            # Уведомляем администраторов об ошибке
            for admin_id in admin_ids:
                try:
                    error_message = f"☠️ Бот перезапускается из-за ошибки: {e}\n\nПроверьте логи для получения дополнительной информации."
                    bot.send_message(admin_id, error_message)
                except Exception as send_e:
                    log_error(f"Не удалось отправить уведомление администратору {admin_id}: {send_e}")
            
            log_info("Попытка перезапуска через 5 секунд...")
            time.sleep(5)

def main():
    """Главная функция запуска бота"""
    try:
        # Приветствие
        log_bright("=" * 60, Fore.MAGENTA)
        log_bright("🎮 STEAM RENTAL BOT", Fore.MAGENTA)
        log_bright("Автоматизированная система аренды Steam аккаунтов", Fore.CYAN)
        log_bright("=" * 60, Fore.MAGENTA)
        
        # Инициализация компонентов
        bot, is_user_authorized, auth_required, admin_ids = initialize_bot()
        if not bot:
            log_error("Не удалось инициализировать бота. Завершение работы.")
            return False
        
        # Инициализация базы данных
        if not initialize_database():
            log_error("Не удалось инициализировать базу данных. Завершение работы.")
            return False
        
        # Инициализация FunPay (необязательно)
        initialize_funpay()
        
        # Инициализация обработчиков
        log_info("Инициализация обработчиков команд...")
        try:
            from tg_utils.handlers import init_handlers
            init_handlers(bot, is_user_authorized, auth_required, admin_ids=admin_ids)
            log_success("Обработчики команд инициализированы")
        except Exception as e:
            log_error(f"Ошибка инициализации обработчиков: {e}")
            return False
        
        # Финальное сообщение перед запуском
        log_bright("🟢 Все компоненты готовы к работе!", Fore.GREEN)
        log_bright("Бот готов принимать команды от пользователей", Fore.CYAN)
        log_bright("-" * 60, Fore.CYAN)
        
        # Запуск бота
        run_bot_polling(bot, admin_ids)
        
    except KeyboardInterrupt:
        log_warning("\n⚠️  Бот остановлен пользователем")
        return True
    except Exception as e:
        log_error(f"Критическая ошибка: {e}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        log_error(f"Неожиданная ошибка при запуске: {e}")
        sys.exit(1)