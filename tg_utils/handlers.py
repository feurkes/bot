# --- Telegram Handlers ---
from tg_utils.keyboards import (
    main_menu, account_kb, game_selection_kb, rental_time_kb, 
    confirmation_kb, navigation_kb, stats_kb, settings_kb, 
    get_game_emoji, back_to_account_kb
)
from tg_utils.helpers import safe_edit_message_text
from tg_utils.state import user_states, user_acc_data, user_data
from tg_utils.db import DB_PATH
from tg_utils.logger import logger
from telebot import types
import sqlite3
import time
import tempfile
import shutil
from datetime import datetime, timedelta
from game_name_mapper import mapper
from steam.steam_account_rental_utils import mark_account_rented, mark_account_free, auto_end_rent, send_account_to_buyer
from utils.email_utils import fetch_steam_guard_code_from_email
import os
import re
import threading
import asyncio
from steam.steam_logout import steam_logout_all_sessions
from tg_utils.config import ADMIN_IDS as CONFIG_ADMIN_IDS
from playwright.async_api import async_playwright
import string
import html
from dotenv import load_dotenv
from telebot import TeleBot

# Загружаем переменные из .env
load_dotenv()

# Получаем токен бота
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_TOKEN")

# Импортируем ADMIN_IDS из конфига
from tg_utils.config import ADMIN_IDS

# Создаем экземпляр бота
bot = TeleBot(TG_TOKEN)

is_user_authorized = None
auth_required = None


# --- Основная функция для инициализации всех обработчиков ---
def init_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """
    Инициализация всех обработчиков бота.
    
    Args:
        bot_instance: Экземпляр TeleBot
        is_user_authorized_func: Функция проверки авторизации пользователя
        auth_required_decorator: Декоратор для проверки авторизации
        admin_ids: Список ID администраторов
    """
    global bot, is_user_authorized, auth_required
    
    bot = bot_instance
    is_user_authorized = is_user_authorized_func
    auth_required = auth_required_decorator or (lambda func: func)  # Default no-op decorator
    
    logger.info("🚀 Инициализация обработчиков Telegram бота...")
    
    # Импортируем и инициализируем все модули обработчиков
    from .handlers import init_handlers as init_all_handlers
    
    try:
        init_all_handlers(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
        logger.info("✅ Все обработчики успешно инициализированы")
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации обработчиков: {e}")
        raise

    @bot.middleware_handler(update_types=['message', 'callback_query'])
    def middleware_handler(bot_instance, message):
        """Middleware для обработки всех сообщений"""
        try:
            user_id = message.from_user.id if hasattr(message, 'from_user') else None
            if user_id:
                logger.debug(f"[MIDDLEWARE] Обработка сообщения от пользователя {user_id}")
        except Exception as e:
            logger.error(f"[MIDDLEWARE] Ошибка в middleware: {e}")

    def error_handler(bot_instance, message):
        """Обработчик ошибок"""
        try:
            logger.error(f"[ERROR_HANDLER] Ошибка обработки сообщения: {message}")
            if hasattr(message, 'chat') and hasattr(message.chat, 'id'):
                bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте позже.")
        except Exception as e:
            logger.error(f"[ERROR_HANDLER] Критическая ошибка в error_handler: {e}")

    # Регистрируем обработчик ошибок
    bot.exception_handler = error_handler

    # Добавляем обработчик для выхода из системы
    @bot.message_handler(commands=['logout'])
    @auth_required
    def cmd_self_logout(message):
        """Команда для выхода пользователя из всех Steam сессий"""
        try:
            user_id = message.from_user.id
            logger.info(f"[LOGOUT] Пользователь {user_id} запросил выход из всех Steam сессий")
            
            # Получаем все аккаунты пользователя
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT login FROM accounts")
            accounts = c.fetchall()
            conn.close()
            
            if not accounts:
                bot.send_message(message.chat.id, "❌ Нет доступных аккаунтов для выхода.")
                return
            
            bot.send_message(message.chat.id, f"⏳ Выполняется выход из {len(accounts)} Steam аккаунтов...")
            
            def logout_worker():
                success_count = 0
                for account in accounts:
                    login = account[0]
                    try:
                        steam_logout_all_sessions(login)
                        success_count += 1
                        logger.info(f"[LOGOUT] Успешный выход из аккаунта {login}")
                    except Exception as e:
                        logger.error(f"[LOGOUT] Ошибка выхода из аккаунта {login}: {e}")
                
                bot.send_message(message.chat.id, 
                    f"✅ Выход выполнен для {success_count} из {len(accounts)} аккаунтов")
            
            threading.Thread(target=logout_worker).start()
            
        except Exception as e:
            logger.error(f"[LOGOUT] Ошибка в cmd_self_logout: {e}")
            bot.send_message(message.chat.id, "❌ Ошибка при выполнении выхода.")

    @bot.message_handler(commands=['logout_user'])
    @auth_required
    def cmd_logout_user(message):
        """Команда для выхода указанного пользователя"""
        try:
            parts = message.text.split()
            if len(parts) < 2:
                bot.send_message(message.chat.id, "❌ Укажите логин: /logout_user <login>")
                return
            
            login = parts[1]
            logger.info(f"[LOGOUT_USER] Запрос выхода пользователя {login}")
            
            def logout_worker():
                try:
                    steam_logout_all_sessions(login)
                    bot.send_message(message.chat.id, f"✅ Пользователь {login} вышел из всех Steam сессий")
                    logger.info(f"[LOGOUT_USER] Успешный выход пользователя {login}")
                except Exception as e:
                    logger.error(f"[LOGOUT_USER] Ошибка выхода пользователя {login}: {e}")
                    bot.send_message(message.chat.id, f"❌ Ошибка при выходе пользователя {login}")
            
            threading.Thread(target=logout_worker).start()
            
        except Exception as e:
            logger.error(f"[LOGOUT_USER] Ошибка в cmd_logout_user: {e}")
            bot.send_message(message.chat.id, "❌ Ошибка при выполнении команды.")

    # Fallback обработчик для всех остальных сообщений
    @bot.message_handler(func=lambda m: True)
    def fallback(message):
        """Fallback обработчик для неопознанных сообщений"""
        try:
            if is_user_authorized and not is_user_authorized(message.chat.id):
                bot.send_message(message.chat.id, 
                    "⛔ У вас нет доступа к этому боту.\n"
                    "📞 Обратитесь к администратору для получения доступа.")
                return
            
            logger.debug(f"[FALLBACK] Неопознанное сообщение от {message.from_user.id}: {message.text}")
            bot.send_message(message.chat.id, 
                "❓ Команда не распознана.\n"
                "Используйте /start для возврата в главное меню.", 
                reply_markup=main_menu())
        except Exception as e:
            logger.error(f"[FALLBACK] Ошибка в fallback обработчике: {e}")

    logger.info("✅ Инициализация основных обработчиков завершена")