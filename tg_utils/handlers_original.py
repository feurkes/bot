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

# --- Вспомогательные функции ---
def parse_imap_host_port(imap_str):
    if ':' in imap_str:
        host, port = imap_str.rsplit(':', 1)
        try:
            port = int(port)
        except ValueError:
            port = None
        return host.strip(), port
    return imap_str.strip(), None

def finalize_add_account(message, user_id):
    if user_id not in user_acc_data:
        user_acc_data[user_id] = {}
        bot.send_message(message.chat.id, f"❌ Ошибка: отсутствуют данные для создания аккаунта", reply_markup=main_menu())
        user_states[user_id] = None
        return
    data = user_acc_data[user_id]
    required_fields = ["id", "login", "password", "game_name"]
    missing_fields = [field for field in required_fields if field not in data]
    if missing_fields:
        bot.send_message(message.chat.id, f"❌ Ошибка: отсутствуют обязательные поля: {', '.join(missing_fields)}", reply_markup=main_menu())
        user_states[user_id] = None
        user_acc_data[user_id] = {}
        return
    try:
        from game_name_mapper import mapper
        normalized = mapper.normalize(data["game_name"])
        data["game_name"] = normalized
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO accounts (id, login, password, game_name, rented_until, status, tg_user_id, email_login, email_password, imap_host) VALUES (?, ?, ?, ?, NULL, 'free', NULL, ?, ?, ?)",
                  (data["id"], data["login"], data["password"], data["game_name"], data.get("email_login"), data.get("email_password"), data.get("imap_host")))
        conn.commit()
        conn.close()
        success_text = (
            f"🎉 <b>Аккаунт успешно добавлен!</b>\n\n"
            f"📋 <b>Данные аккаунта:</b>\n"
            f"• ID: <code>{data['id']}</code>\n"
            f"• Логин: <code>{data['login']}</code>\n"
            f"• Игра: <code>{data['game_name']}</code>\n"
            f"• Статус: <code>Свободен</code>\n"
        )
        
        if data.get("email_login"):
            success_text += f"• Email: <code>{data['email_login']}</code>\n"
            success_text += f"• IMAP: <code>{data.get('imap_host', 'Не указан')}</code>\n"
        else:
            success_text += "• Email: <code>Не настроен</code>\n"
            
        success_text += (
            f"\n✅ Аккаунт готов к использованию!\n"
            f"🎮 Теперь вы можете управлять им через \"Управление аккаунтами\""
        )
        
        bot.send_message(message.chat.id, success_text, parse_mode="HTML", reply_markup=main_menu())
    except Exception as e:
        logger.error(f"Ошибка при добавлении аккаунта: {e}")
        bot.send_message(message.chat.id, f"❌ Ошибка: {e}", reply_markup=main_menu())
    user_states[user_id] = None
    user_acc_data[user_id] = {}

def send_steam_success_log(page, chat_id, login=None, password=None):
    try:
        if login and password:
            escaped_login = html.escape(login)
            escaped_password = html.escape(password)
            bot.send_message(chat_id, f"✅ <b>Успешный вход в Steam!</b>\n\nЛогин: <code>{escaped_login}</code>\nПароль: <code>{escaped_password}</code>", parse_mode="HTML")
        else:
            bot.send_message(chat_id, "✅ <b>Успешный вход в Steam!</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка при отправке лога: {e}")

# --- Основная функция для инициализации всех обработчиков ---
def init_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    global bot, is_user_authorized, auth_required
    bot = bot_instance
    is_user_authorized = is_user_authorized_func
    auth_required = auth_required_decorator
    
    # Если переданы дополнительные админы, добавляем их без дублирования
    if admin_ids:
        global ADMIN_IDS
        ADMIN_IDS = list(set(ADMIN_IDS + admin_ids))

    # Обработчик ошибок для отправки уведомлений админам
    @bot.middleware_handler(update_types=['message', 'callback_query'])
    def error_handler(bot_instance, message):
        try:
            if message and hasattr(message, 'error'):
                error_text = f"❌ Произошла ошибка в боте:\n\n{str(message.error)}"
                for admin_id in ADMIN_IDS:
                    try:
                        bot.send_message(admin_id, error_text)
                    except Exception as e:
                        print(f"Ошибка при отправке уведомления об ошибке админу {admin_id}: {str(e)}")
        except Exception as e:
            print(f"Ошибка в обработчике ошибок: {str(e)}")

    # Отправляем уведомление о запуске бота админам
    logger.info("✅ Бот успешно запущен и готов к работе! ✅")
    for admin_id in ADMIN_IDS:
        try:
            bot.send_message(
                admin_id, 
                "🤖 Бот успешно запущен и готов к работе!", 
                reply_markup=main_menu(),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомления админу {admin_id}: {str(e)}")

    # --- ГЛАВНОЕ МЕНЮ ---
    @bot.message_handler(commands=['start', 'menu'])
    def cmd_start(message):
        if is_user_authorized and is_user_authorized(message.chat.id):
            welcome_text = (
                "🚀 <b>Steam Rental Bot v2.0</b>\n\n"
                "💡 <b>Добро пожаловать в систему управления арендой Steam аккаунтов!</b>\n\n"
                "📋 <b>Управление аккаунтами</b> - просмотр, добавление и настройка аккаунтов\n"
                "➕ <b>Добавить новый аккаунт</b> - регистрация нового Steam аккаунта\n"
                "📊 <b>Статистика и аналитика</b> - отчеты по аренде и доходам\n"
                "⚙️ <b>Настройки системы</b> - конфигурация уведомлений и безопасности\n"
                "💬 <b>Техподдержка</b> - связь с администратором\n\n"
                "🔥 Выберите действие для начала работы:"
            )
            bot.send_message(message.chat.id, welcome_text, reply_markup=main_menu(), parse_mode="HTML")
        else:
            unauthorized_text = (
                "⛔ <b>Доступ ограничен</b>\n\n"
                "🔐 Этот бот предназначен только для авторизованных администраторов системы аренды Steam аккаунтов.\n\n"
                "📞 Для получения доступа обратитесь к администратору."
            )
            bot.send_message(message.chat.id, unauthorized_text, parse_mode="HTML")

    # --- ТЕСТ СТИМ-АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("test:"))
    def cb_test_account(call):
        bot.answer_callback_query(call.id)
        acc_id = call.data.split(":")[1]
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT login, password, email_login, email_password, imap_host FROM accounts WHERE id=?", (acc_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
            return
        login, password, email_login, email_password, imap_host = row
        bot.send_message(call.message.chat.id, "🧪 Тест запущен! Ожидайте отчёт.")
        bot.send_message(call.message.chat.id, f"🧪 <b>Тест аккаунта {acc_id}...</b>", parse_mode="HTML")

        async def run_test():
            # --- ЛОГИКА ТЕСТА ИЗ СТАРОЙ ВЕРСИИ --- (ВКЛЮЧАЯ PLAYWRIGHT)
            from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
            from utils.browser_config import get_browser_config
            import time
            import json
            import os
            from utils.email_utils import fetch_steam_guard_code_from_email # Убедитесь, что email_utils доступен

            SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sessions')
            os.makedirs(SESSIONS_DIR, exist_ok=True)
            session_file = os.path.join(SESSIONS_DIR, f"steam_{login}.json")
            browser = None
            context = None
            page = None
            try:
                bot.send_message(call.message.chat.id, "🧪 Открываю браузер и страницу Steam...") # Добавил сообщение о запуске браузера
                browser_config = get_browser_config()
                with sync_playwright() as p:
                    browser = p.chromium.launch(**browser_config)
                    context = None
                    logged_in = False
                    
                    # --- Попытка использовать сохраненную сессию ---
                    if os.path.exists(session_file):
                        logger.info(f"[STEAM-TEST] Найден файл сессии для {login}, пробуем восстановить сессию")
                        context = None
                        page = None
                        
                        try:
                            # Создаем контекст с сохраненным состоянием
                            logger.info(f"[STEAM-TEST] Загружаю сохраненную сессию для {login}")
                            context = browser.new_context(storage_state=session_file)
                            page = context.new_page()
                            
                            # Переходим на страницу аккаунта
                            logger.info(f"[STEAM-TEST] Перехожу на страницу аккаунта Steam для проверки сессии")
                            await page.goto("https://store.steampowered.com/account/", wait_until='networkidle')
                            
                            # Делаем скриншот для диагностики
                            try:
                                session_screenshot_path = os.path.join(screenshots_dir, f'session_check_{acc_id}.png')
                                await page.screenshot(path=session_screenshot_path)
                                logger.info(f"[STEAM-TEST] Скриншот проверки сессии создан")
                            except Exception as screenshot_e:
                                logger.warning(f"[STEAM-TEST] Ошибка при создании скриншота сессии: {screenshot_e}")
                            
                            # Расширенная проверка активной сессии
                            logger.info(f"[STEAM-TEST] Проверяю активность сессии...")
                            
                            # Множественные селекторы для проверки входа
                            success_selectors = [
                                "#account_pulldown",           # Основной селектор
                                ".playerAvatar",               # Аватар игрока
                                ".username",                   # Имя пользователя
                                "[class*='account']",          # Любой класс с 'account'
                                "a[href*='logout']",           # Ссылка выхода
                                ".store_nav_area .username",   # Имя в навигации
                                "#account_language_pulldown"   # Селектор языка аккаунта
                            ]
                            
                            logged_in = False
                            found_selector = None
                            
                            for selector in success_selectors:
                                try:
                                    logger.debug(f"[STEAM-TEST] Проверяю селектор: {selector}")
                                    element = await page.wait_for_selector(selector, timeout=5000)
                                    
                                    if element:
                                        # Дополнительная проверка - получаем текст элемента если возможно
                                        try:
                                            element_text = await element.inner_text()
                                            logger.info(f"[STEAM-TEST] ✅ Найден элемент входа: {selector} (текст: '{element_text[:50]}')")
                                        except:
                                            logger.info(f"[STEAM-TEST] ✅ Найден элемент входа: {selector}")
                                        
                                        logged_in = True
                                        found_selector = selector
                                        break
                                        
                                except Exception as e:
                                    logger.debug(f"[STEAM-TEST] Селектор {selector} не найден: {e}")
                                    continue
                            
                            if logged_in:
                                logger.info(f"[STEAM-TEST] ✅ Сессия активна через селектор: {found_selector}")
                                
                                # Дополнительная проверка через URL
                                current_url = page.url
                                logger.info(f"[STEAM-TEST] Текущий URL: {current_url}")
                                
                                # Проверяем, не перенаправило ли на страницу входа
                                if "login" in current_url.lower():
                                    logger.warning(f"[STEAM-TEST] ⚠️ URL содержит 'login', возможно сессия неактивна")
                                    logged_in = False
                                
                                # Проверяем cookies
                                try:
                                    cookies = await page.context.cookies()
                                    steam_cookies = [c for c in cookies if 'steamLoginSecure' in c.get('name', '')]
                                    if steam_cookies:
                                        logger.info(f"[STEAM-TEST] ✅ Найдены активные Steam cookies")
                                    else:
                                        logger.warning(f"[STEAM-TEST] ⚠️ Steam cookies не найдены")
                                except Exception as cookie_e:
                                    logger.warning(f"[STEAM-TEST] Ошибка проверки cookies: {cookie_e}")
                                
                                if logged_in:
                                    # Отправляем скриншот успешной сессии
                                    try:
                                        with open(session_screenshot_path, 'rb') as photo:
                                            bot.send_photo(
                                                call.message.chat.id, 
                                                photo, 
                                                caption=f"[STEAM][LOGIN: {html.escape(login)}] ✅ Сессия активна"
                                            )
                                    except Exception as send_e:
                                        logger.warning(f"[STEAM-TEST] Ошибка отправки скриншота: {send_e}")
                                    
                                    bot.send_message(call.message.chat.id, "✅ <b>Сессия активна. Вход выполнен!</b>", parse_mode="HTML")
                                    logger.info(f"[STEAM-TEST] Успешно восстановлена сессия для {login}")
                                    
                                    try:
                                        send_steam_success_log(page, call.message.chat.id, login, password)
                                    except Exception as log_e:
                                        logger.error(f"[STEAM-TEST] Ошибка отправки лога успеха: {log_e}")
                                    
                                    return # Успех через сессию, завершаем тест
                            
                            # Если дошли сюда, сессия неактивна
                            logger.warning(f"[STEAM-TEST] ❌ Сессия неактивна - элементы входа не найдены")
                            
                            # Проверяем, есть ли форма входа на странице
                            login_form_selectors = [
                                "input[type='password']",
                                ".loginbox",
                                ".newlogindialog", 
                                "button[type='submit']"
                            ]
                            
                            login_form_found = False
                            for selector in login_form_selectors:
                                try:
                                    element = await page.query_selector(selector)
                                    if element:
                                        logger.info(f"[STEAM-TEST] Найдена форма входа: {selector}")
                                        login_form_found = True
                                        break
                                except:
                                    continue
                            
                            if login_form_found:
                                logger.info(f"[STEAM-TEST] Обнаружена форма входа, сессия точно неактивна")
                            
                            # Отправляем диагностический скриншот
                            try:
                                with open(session_screenshot_path, 'rb') as photo:
                                    bot.send_photo(
                                        call.message.chat.id, 
                                        photo, 
                                        caption=f"[STEAM][LOGIN: {html.escape(login)}] ❌ Сессия неактивна"
                                    )
                            except Exception as send_e:
                                logger.warning(f"[STEAM-TEST] Ошибка отправки диагностического скриншота: {send_e}")

                        except Exception as e:
                            # Если сессия не сработала, логируем и продолжаем попытку логина
                            error_msg = f"[STEAM-TEST] Не удалось использовать сессию для {login}: {str(e)}"
                            logger.warning(error_msg, exc_info=True)
                            
                            # Создаем скриншот ошибки если возможно
                            if page:
                                try:
                                    error_screenshot_path = os.path.join(screenshots_dir, f'session_error_{acc_id}.png')
                                    await page.screenshot(path=error_screenshot_path)
                                    with open(error_screenshot_path, 'rb') as photo:
                                        bot.send_photo(
                                            call.message.chat.id, 
                                            photo, 
                                            caption=f"[STEAM][LOGIN: {html.escape(login)}] ❌ Ошибка проверки сессии"
                                        )
                                except Exception as screenshot_e:
                                    logger.warning(f"[STEAM-TEST] Ошибка создания скриншота ошибки: {screenshot_e}")
                            
                            bot.send_message(call.message.chat.id, "⚠️ Сохраненная сессия неактивна или повреждена. Пробую полный логин.", parse_mode="HTML")
                            
                        finally:
                            # Закрываем контекст в любом случае если он был создан
                            try:
                                if context:
                                    await context.close()
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Контекст закрыт")
                                
                                # Удаляем временную папку
                                if 'user_data_dir' in locals() and os.path.exists(user_data_dir):
                                    shutil.rmtree(user_data_dir, ignore_errors=True)
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Временные данные браузера удалены")
                                    
                            except Exception as e:
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ошибка очистки: {e}")

                            
                            context = None
                            page = None

                    # Если дошли до этой точки, переходим к полному логину
                    logger.info(f"[STEAM-TEST] Переходим к полному процессу логина для {login}")


                    # --- Полный логин, если сессия не сработала ---
                    if context is None:
                        context = browser.new_context(
                            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                            viewport=None,
                            locale="ru-RU",
                            java_script_enabled=True,
                            ignore_https_errors=True
                        )
                        page = context.new_page()
                        page.goto("https://store.steampowered.com/login/")
                        page.wait_for_load_state("networkidle")
                        # Сохраняем скриншот и HTML страницы логина
                        debug_png_path_step1 = os.path.join(SESSIONS_DIR, f"steam_login_step1_{login}.png")
                        page.screenshot(path=debug_png_path_step1)
                        bot.send_photo(call.message.chat.id, open(debug_png_path_step1, "rb"), caption=f"[{login}] Скриншот: страница логина Steam")


                    # Проверка, что мы на странице логина или аккаунта, иначе ошибка редиректа
                    if "login" not in page.url and "account" not in page.url:
                        html = page.content()
                        debug_html_path = os.path.join(SESSIONS_DIR, f"steam_login_redirect_error_{login}.html")
                        with open(debug_html_path, "w", encoding="utf-8") as f: f.write(html)
                        debug_png_path = os.path.join(SESSIONS_DIR, f"steam_login_redirect_error_{login}.png")
                        page.screenshot(path=debug_png_path)
                        bot.send_message(call.message.chat.id, "❌ <b>Steam редиректит не на страницу логина или аккаунта. Возможно, проблема с IP или аккаунтом. Скриншот и HTML сохранены для отладки.</b>", parse_mode="HTML")
                        bot.send_photo(call.message.chat.id, open(debug_png_path, "rb"), caption=f"[{login}] Скриншот редиректа")
                        bot.send_document(call.message.chat.id, open(debug_html_path, "rb"), caption=f"[{login}] HTML страницы редиректа")
                        return # Завершаем тест при ошибке
                    
                    # Если мы на странице логина, вводим данные
                    if "login" in page.url:
                        bot.send_message(call.message.chat.id, "🧪 Жду появления поля логина...") # Добавлено сообщение
                        try:
                            # Ожидаем поле логина с увеличенным таймаутом
                            page.wait_for_selector('input[type="text"]', timeout=30000) # Таймаут 30с
                        except PWTimeoutError:
                            # Сохраняем HTML и скриншот при таймауте ожидания поля логина
                            html = page.content()
                            debug_html_path = os.path.join(SESSIONS_DIR, f"steam_login_timeout_field_{login}.html")
                            with open(debug_html_path, "w", encoding="utf-8") as f: f.write(html)
                            debug_png_path = os.path.join(SESSIONS_DIR, f"steam_login_timeout_field_{login}.png")
                            page.screenshot(path=debug_png_path)
                            bot.send_message(call.message.chat.id, "❌ <b>Поле логина не найдено на странице за 30 секунд. Steam мог временно заблокировать доступ или страница изменилась. HTML и скриншот сохранены.</b>", parse_mode="HTML")
                            bot.send_photo(call.message.chat.id, open(debug_png_path, "rb"), caption=f"[{login}] Поле логина не найдено")
                            bot.send_document(call.message.chat.id, open(debug_html_path, "rb"), caption=f"[{login}] HTML ошибки поля логина")
                            return # Завершаем тест при ошибке
                            
                        bot.send_message(call.message.chat.id, "🧪 Поле найдено! Ввожу логин и пароль...") # Добавлено сообщение
                        page.fill('input[type="text"]', login)
                        page.fill('input[type="password"]', password)
                        # Сохраняем скриншот после ввода данных
                        debug_png_path_step2 = os.path.join(SESSIONS_DIR, f"steam_login_step2_{login}.png")
                        page.screenshot(path=debug_png_path_step2)
                        bot.send_photo(call.message.chat.id, open(debug_png_path_step2, "rb"), caption=f"[{login}] Скриншот: логин и пароль введены")
                        
                        page.click("button[type='submit']")
                        
                        try:
                            # Ожидаем либо Steam Guard, либо успех, либо ошибку после ввода лог/пасс
                            # Увеличим таймаут ожидания ответа от Steam
                            page.wait_for_selector("#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=30000) # Таймаут 30с
                        except PWTimeoutError:
                            # Сохраняем скриншоты и HTML при таймауте после ввода лог/пасс
                            bot.send_message(call.message.chat.id, "❌ <b>Время ожидания ответа от Steam истекло после ввода логина/пароля (30 секунд).</b>", parse_mode="HTML")
                            debug_png_path = os.path.join(SESSIONS_DIR, f"steam_login_fail_timeout_logpass_{login}.png")
                            page.screenshot(path=debug_png_path)
                            debug_html_path = os.path.join(SESSIONS_DIR, f"steam_login_fail_timeout_logpass_{login}.html")
                            with open(debug_html_path, "w", encoding="utf-8") as f: f.write(page.content())
                            bot.send_photo(call.message.chat.id, open(debug_png_path, "rb"), caption=f"[{login}] Таймаут после ввода лог/пасс")
                            bot.send_document(call.message.chat.id, open(debug_html_path, "rb"), caption=f"[{login}] HTML таймаута")
                            return # Завершаем тест при ошибке

                        # Проверяем состояние страницы после ввода лог/пасс
                        need_guard = False
                        if page.query_selector("#auth_buttonset_entercode"): need_guard = True
                        elif page.query_selector("input[maxlength='1']"): need_guard = True
                        elif "Введите код, полученный на электронный адрес" in page.content(): need_guard = True # Проверка текста на странице

                        if need_guard:
                            bot.send_message(call.message.chat.id, "⚠️ Требуется ввод Steam Guard кода.", parse_mode="HTML") # Добавлено сообщение
                            if not (email_login and email_password and imap_host):
                                bot.send_message(call.message.chat.id, f"❌ <b>Для этого аккаунта ({login}) требуется Steam Guard, но данные почты не настроены!</b>", parse_mode="HTML")
                                # Сохраняем скриншот и HTML при требовании Guard без данных почты
                                debug_png_path = os.path.join(SESSIONS_DIR, f"steam_guard_required_no_mail_{login}.png")
                                page.screenshot(path=debug_png_path)
                                debug_html_path = os.path.join(SESSIONS_DIR, f"steam_guard_required_no_mail_{login}.html")
                                with open(debug_html_path, "w", encoding="utf-8") as f: f.write(page.content())
                                bot.send_photo(call.message.chat.id, open(debug_png_path, "rb"), caption=f"[{login}] Требуется Guard, нет данных почты")
                                bot.send_document(call.message.chat.id, open(debug_html_path, "rb"), caption=f"[{login}] HTML Guard без почты")
                                return # Завершаем тест при ошибке

                            bot.send_message(call.message.chat.id, "🧪 Получаю код Steam Guard с почты...", parse_mode="HTML")
                            # Сохраняем скриншот страницы Steam Guard
                            debug_png_path_step3 = os.path.join(SESSIONS_DIR, f"steam_login_step3_guard_page_{login}.png")
                            page.screenshot(path=debug_png_path_step3)
                            bot.send_photo(call.message.chat.id, open(debug_png_path_step3, "rb"), caption=f"[{login}] Скриншот: страница ввода кода Steam Guard")

                            # --- ПОЛУЧЕНИЕ КОДА GUARD С ПОЧТЫ ---
                            # Используем fetch_steam_guard_code_from_email с таймаутом и логированием
                            import time
                            code = None
                            start_time = time.time()
                            # Увеличим таймаут для получения кода по почте до 120 секунд
                            email_fetch_timeout = 120 # Увеличил таймаут до 120 секунд
                            bot.send_message(call.message.chat.id, f"⏳ Ожидание Steam Guard кода с почты {email_login} (до {email_fetch_timeout} секунд). Убедитесь, что IMAP работает и бот имеет доступ к почте.", parse_mode="HTML") # Уточнил сообщение

                            while (time.time() - start_time) < email_fetch_timeout: # Ожидаем до email_fetch_timeout секунд
                                try:
                                    # Убедимся, что parse_imap_host_port правильно передает host и port
                                    imap_host_clean, imap_port_clean = parse_imap_host_port(imap_host)

                                    # Увеличим таймаут для каждой попытки получения кода по почте до 10 секунд
                                    code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host_clean, imap_port_clean, logger=logger, mode='login') # Короткий таймаут на каждую попытку
                                    if code: break
                                except Exception as email_ex:
                                    logger.error(f"Ошибка при получении кода Steam Guard с почты: {email_ex}")
                                time.sleep(4) # Пауза между попытками увеличена до 4 секунд

                            if not code:
                                bot.send_message(call.message.chat.id, f"❌ <b>Не удалось получить код Steam Guard с почты {email_login} за {email_fetch_timeout} секунд! Проверьте логин/пароль почты, IMAP сервер и доступ бота к почте.</b>", parse_mode="HTML") # Уточнил сообщение
                                # Сохраняем скриншот и HTML при неудаче получения кода
                                debug_png_path = os.path.join(SESSIONS_DIR, f"steam_guard_fetch_fail_{login}.png")
                                page.screenshot(path=debug_png_path)
                                debug_html_path = os.path.join(SESSIONS_DIR, f"steam_guard_fetch_fail_{login}.html")
                                with open(debug_html_path, "w", encoding="utf-8") as f: f.write(page.content())
                                bot.send_photo(call.message.chat.id, open(debug_png_path, "rb"), caption=f"[{login}] Не удалось получить Guard")
                                bot.send_document(call.message.chat.id, open(debug_html_path, "rb"), caption=f"[{login}] HTML при ошибке Guard")
                                return # Завершаем тест при ошибке

                            bot.send_message(call.message.chat.id, f"🧪 Ввожу код Steam Guard: <code>{code}</code>", parse_mode="HTML")

                            # --- ВВОД КОДА GUARD ---
                            # Ввод кода Guard с защитой от разрушения контекста
                            try:
                                # Проверяем доступность страницы перед работой с элементами
                                page.wait_for_load_state('domcontentloaded', timeout=5000)
                                
                                if page.query_selector("input[maxlength='1']"):
                                    inputs = page.query_selector_all("input[maxlength='1']")
                                    if len(inputs) == len(code): # Проверка на совпадение количества полей и длины кода
                                        for i, ch in enumerate(code):
                                            inputs[i].fill(ch)
                                            time.sleep(0.1)  # Небольшая пауза между символами
                                        time.sleep(1) # Пауза после полного ввода
                                    else:
                                        bot.send_message(call.message.chat.id, "❌ <b>Ошибка: количество полей для кода Steam Guard не соответствует ожидаемому.</b>", parse_mode="HTML")
                                        return # Завершаем тест при ошибке

                                elif page.query_selector("input[name='authcode']"):
                                    page.fill("input[name='authcode']", code)
                                    time.sleep(1) # Небольшая пауза после ввода
                                else:
                                    bot.send_message(call.message.chat.id, "❌ <b>Не найдено поле для ввода кода Steam Guard.</b>", parse_mode="HTML")
                                    return # Завершаем тест при ошибке

                                # Steam автоматически отправляет код после полного ввода
                                # Просто ждем обработки без поиска кнопки отправки
                                bot.send_message(call.message.chat.id, "⏳ <b>Код введен, ожидаю обработки Steam...</b>", parse_mode="HTML")
                                time.sleep(3) # Пауза для обработки кода Steam
                                
                            except Exception as context_err:
                                logger.error(f"[TEST] Ошибка контекста при вводе кода: {context_err}")
                                bot.send_message(call.message.chat.id, "❌ <b>Ошибка контекста браузера при вводе кода. Попробуйте позже.</b>", parse_mode="HTML")
                                return

                            try:
                                # Ожидаем результат с повторными попытками при ошибках контекста
                                for attempt in range(3):
                                    try:
                                        page.wait_for_selector("#account_pulldown, .newlogindialog_FormError", timeout=15000)
                                        break
                                    except Exception as wait_err:
                                        if "Execution context was destroyed" in str(wait_err) and attempt < 2:
                                            logger.warning(f"[TEST] Контекст разрушен, попытка {attempt + 1}/3")
                                            time.sleep(2)
                                            continue
                                        else:
                                            raise wait_err

                                # Проверяем результат входа
                                if page.query_selector("#account_pulldown"):
                                    # Сохраняем storage_state (сессию) для этого аккаунта
                                    try:
                                        context.storage_state(path=session_file)
                                        logger.info(f"[STEAM-SESSION] Сессия сохранена: {session_file}")
                                    except Exception as ex:
                                        logger.warning(f"[STEAM-SESSION] Не удалось сохранить storage_state: {ex}")

                                    bot.send_message(call.message.chat.id, "✅ <b>Вход выполнен после ввода Steam Guard!</b>", parse_mode="HTML")
                                    send_steam_success_log(page, call.message.chat.id, login, password)
                                    return # Успех, завершаем тест

                                elif page.query_selector(".newlogindialog_FormError"):
                                    err = page.inner_text(".newlogindialog_FormError")
                                    bot.send_message(call.message.chat.id, f"❌ <b>Ошибка входа после ввода Steam Guard:</b> {err}", parse_mode="HTML")
                                    return
                                else:
                                    bot.send_message(call.message.chat.id, "❌ <b>Неожиданное состояние после ввода кода. Проверьте аккаунт вручную.</b>", parse_mode="HTML")
                                    return

                            except Exception as final_err:
                                logger.error(f"[TEST] Финальная ошибка при проверке результата: {final_err}")
                                if "Execution context was destroyed" in str(final_err):
                                    bot.send_message(call.message.chat.id, "❌ <b>Контекст браузера был разрушен при проверке результата. Тест завершен досрочно.</b>", parse_mode="HTML")
                                else:
                                    bot.send_message(call.message.chat.id, f"❌ <b>Ошибка при проверке результата входа:</b> {final_err}", parse_mode="HTML")
                                return


                        # Если Steam Guard не нужен (проверяем результат сразу после ввода лог/пасс)
                        elif page.query_selector("#account_pulldown"):
                            # Сохраняем storage_state (сессию) для этого аккаунта
                            try:
                                context.storage_state(path=session_file)
                                logger.info(f"[STEAM-SESSION] Сессия сохранена: {session_file}")
                            except Exception as ex:
                                logger.warning(f"[STEAM-SESSION] Не удалось сохранить storage_state: {ex}")

                            bot.send_message(call.message.chat.id, "✅ <b>Вход выполнен без Steam Guard!</b>", parse_mode="HTML")
                            send_steam_success_log(page, call.message.chat.id, login, password)
                            return # Успех, завершаем тест

                # Если дошли сюда, значит тест не завершился успешно
                # Это может произойти, если были переходы не на ожидаемые страницы
                bot.send_message(call.message.chat.id, "❌ <b>Тест аккаунта завершился неожиданно. Проверьте логи.</b>", parse_mode="HTML")

            except Exception as e:
                # Безопасный вывод ошибки без HTML-тегов
                error_msg = str(e)
                # logger.error(f"Общая ошибка при тестировании аккаунта {acc_id}: {error_msg}", exc_info=True) # Логирование в файл
                bot.send_message(call.message.chat.id, f"❌ <b>Внутренняя ошибка при выполнении теста:</b> {error_msg}", parse_mode="HTML")

            finally:
                # Закрываем браузер и контекст в любом случае
                try:
                    if context:
                        context.close()
                except:
                    pass
                try:
                    if browser:
                        browser.close()
                except:
                    pass

        # Запускаем тест в отдельном потоке
        import threading
        threading.Thread(target=run_test, daemon=True).start()

    # --- ОБНОВЛЕНИЕ МЕНЮ ---
    @bot.callback_query_handler(func=lambda c: c.data == "refresh_menu")
    @auth_required
    def cb_refresh(call):
        bot.answer_callback_query(call.id)
        bot.edit_message_text("👾 <b>Steam Rental 1.0.3</b>\nУправляй аккаунтами и арендой!", 
                            call.message.chat.id, call.message.message_id, 
                            reply_markup=main_menu(), parse_mode="HTML")

    # --- НАЗАД В МЕНЮ ---
    @bot.callback_query_handler(func=lambda c: c.data == "back_to_menu")
    @auth_required
    def cb_back(call):
        bot.answer_callback_query(call.id)
        bot.edit_message_text("👾 <b>Steam Rental 1.0.3</b>\nУправляй аккаунтами и арендой!", 
                            call.message.chat.id, call.message.message_id, 
                            reply_markup=main_menu(), parse_mode="HTML")

    # --- ДОБАВЛЕНИЕ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data == "add_acc")
    @auth_required
    def cb_add_acc(call):
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        user_states[user_id] = "add_id"
        user_acc_data[user_id] = {}
        
        add_acc_text = (
            "🆕 <b>Добавление нового Steam аккаунта</b>\n\n"
            "📋 <b>Шаг 1/6: ID аккаунта</b>\n\n"
            "🔢 Введите уникальный числовой ID для аккаунта\n"
            "💡 Это внутренний номер для идентификации в системе\n\n"
            "📝 <b>Примеры:</b> 1, 2, 123, 9999\n"
            "⚠️ ID должен быть уникальным (не повторяться)"
        )
        bot.edit_message_text(add_acc_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_id")
    @auth_required
    def add_id_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["id"] = message.text.strip()
        user_states[user_id] = "add_login"
        
        login_text = (
            "🔑 <b>Шаг 2/6: Логин Steam</b>\n\n"
            "🎮 Введите логин от вашего Steam аккаунта\n"
            "💡 Это имя пользователя для входа в Steam (не отображаемое имя)\n\n"
            "⚠️ <b>Важно:</b> Логин должен быть точным, так как он используется для автоматического входа\n"
            "📝 <b>Пример:</b> mysteamlogin123"
        )
        bot.send_message(message.chat.id, login_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_login")
    @auth_required
    def add_login_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["login"] = message.text.strip()
        user_states[user_id] = "add_password"
        
        password_text = (
            "🔐 <b>Шаг 3/6: Пароль Steam</b>\n\n"
            "🔒 Введите пароль от вашего Steam аккаунта\n"
            "🛡️ Пароль будет зашифрован и храниться в безопасности\n\n"
            "💡 <b>Совет:</b> Используйте надежный пароль для защиты аккаунта"
        )
        bot.send_message(message.chat.id, password_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_password")
    @auth_required
    def add_password_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["password"] = message.text.strip()
        user_states[user_id] = "add_game"
        
        game_text = (
            "🎮 <b>Шаг 4/6: Название игры</b>\n\n"
            "📋 Введите название игры <b>точь-в-точь как на FunPay</b>\n\n"
            "✅ <b>Правильные примеры:</b>\n"
            "• <code>CS2</code>\n"
            "• <code>DOTA 2</code>\n"
            "• <code>PUBG</code>\n"
            "• <code>Apex Legends</code>\n"
            "• <code>Valorant</code>\n"
            "• <code>Grand Theft Auto V</code>\n\n"
            "⚠️ <b>Важно:</b> Название должно совпадать с категорией на FunPay для корректной работы автоматических заказов\n\n"
            "💡 <b>Совет:</b> Проверьте точное написание на сайте FunPay в разделе игр"
        )
        bot.send_message(message.chat.id, game_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_game")
    @auth_required
    def add_game_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["game_name"] = message.text.strip()
        user_states[user_id] = "add_mail"
        
        mail_text = (
            "📧 <b>Шаг 5/6: Настройка почты</b>\n\n"
            "📨 Введите email для получения Steam Guard кодов\n"
            "🔐 Эта почта должна быть привязана к Steam аккаунту\n\n"
            "✅ <b>Нужно для:</b>\n"
            "• Автоматического получения кодов Steam Guard\n"
            "• Функции \"Получить код\" в управлении аккаунтом\n"
            "• Автоматической аренды без ручного ввода кодов\n\n"
            "💡 Введите <code>нет</code> чтобы пропустить настройку почты"
        )
        bot.send_message(message.chat.id, mail_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail")
    @auth_required
    def add_mail_step(message):
        user_id = message.from_user.id
        if message.text.lower() != "нет":
            user_acc_data[user_id]["email_login"] = message.text.strip()
            user_states[user_id] = "add_mail_pw"
            
            mail_pw_text = (
                "🔑 <b>Пароль от почты</b>\n\n"
                "🔐 Введите пароль от указанной почты\n"
                "📡 Используется для IMAP подключения и получения кодов\n\n"
                "⚠️ <b>Безопасность:</b> Пароль шифруется и надежно хранится\n"
                "💡 Для Gmail используйте \"Пароль приложения\" вместо основного пароля"
            )
            bot.send_message(message.chat.id, mail_pw_text, parse_mode="HTML")
        else:
            finalize_add_account(message, user_id)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail_pw")
    @auth_required
    def add_mail_pw_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["email_password"] = message.text.strip()
        user_states[user_id] = "add_mail_imap"
        
        # Создаем клавиатуру с предустановленными IMAP серверами
        kb = types.InlineKeyboardMarkup()
        
        # Популярные IMAP сервера
        kb.add(types.InlineKeyboardButton("📧 imap.firstmail.ltd", callback_data="imap_preset:imap.firstmail.ltd"))
        kb.add(types.InlineKeyboardButton("📮 Gmail (imap.gmail.com)", callback_data="imap_preset:imap.gmail.com"))
        kb.add(types.InlineKeyboardButton("📬 Yandex (imap.yandex.ru)", callback_data="imap_preset:imap.yandex.ru"))
        kb.add(types.InlineKeyboardButton("📪 Mail.ru (imap.mail.ru)", callback_data="imap_preset:imap.mail.ru"))
        kb.add(types.InlineKeyboardButton("📫 Outlook (outlook.office365.com)", callback_data="imap_preset:outlook.office365.com"))
        kb.add(types.InlineKeyboardButton("✏️ Ввести вручную", callback_data="imap_manual"))
        
        imap_text = (
            "🌐 <b>Шаг 6/6: IMAP сервер</b>\n\n"
            "📡 Выберите IMAP сервер вашего почтового провайдера:\n\n"
            "🔽 <b>Популярные серверы:</b>\n"
            "• <code>imap.firstmail.ltd</code> - FirstMail\n"
            "• <code>imap.gmail.com</code> - Gmail\n"
            "• <code>imap.yandex.ru</code> - Yandex\n"
            "• <code>imap.mail.ru</code> - Mail.ru\n"
            "• <code>outlook.office365.com</code> - Outlook\n\n"
            "💡 Нажмите на нужный сервер или выберите \"Ввести вручную\""
        )
        bot.send_message(message.chat.id, imap_text, reply_markup=kb, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("imap_preset:"))
    @auth_required
    def cb_imap_preset(call):
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        
        if user_id not in user_states or user_states[user_id] != "add_mail_imap":
            bot.send_message(call.message.chat.id, "❌ Ошибка: неожиданное состояние. Начните добавление аккаунта заново.")
            return
            
        imap_host = call.data.split(":", 1)[1]
        user_acc_data[user_id]["imap_host"] = imap_host
        
        bot.edit_message_text(
            f"✅ <b>Выбран IMAP сервер:</b> <code>{imap_host}</code>\n\n"
            "🎯 Настройка почты завершена! Добавляем аккаунт в систему...",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            parse_mode="HTML"
        )
        
        finalize_add_account(call.message, user_id)

    @bot.callback_query_handler(func=lambda c: c.data == "imap_manual")
    @auth_required
    def cb_imap_manual(call):
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        
        if user_id not in user_states or user_states[user_id] != "add_mail_imap":
            bot.send_message(call.message.chat.id, "❌ Ошибка: неожиданное состояние. Начните добавление аккаунта заново.")
            return
            
        user_states[user_id] = "add_mail_imap_manual"
        
        bot.edit_message_text(
            "✏️ <b>Ручной ввод IMAP сервера</b>\n\n"
            "🌐 Введите адрес IMAP сервера вашего провайдера\n\n"
            "📝 <b>Формат:</b> <code>imap.example.com</code>\n"
            "⚠️ Без <code>https://</code> и других префиксов\n\n"
            "💡 <b>Примеры:</b>\n"
            "• <code>imap.gmail.com</code>\n"
            "• <code>imap.yandex.ru</code>\n"
            "• <code>imap.firstmail.ltd</code>",
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            parse_mode="HTML"
        )

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail_imap_manual")
    @auth_required
    def add_mail_imap_manual_step(message):
        user_id = message.from_user.id
        host, port = parse_imap_host_port(message.text.strip())
        user_acc_data[user_id]["imap_host"] = host
        if port:
            user_acc_data[user_id]["imap_port"] = port
        finalize_add_account(message, user_id)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail_imap")
    @auth_required
    def add_mail_imap_step(message):
        user_id = message.from_user.id
        host, port = parse_imap_host_port(message.text.strip())
        user_acc_data[user_id]["imap_host"] = host
        if port:
            user_acc_data[user_id]["imap_port"] = port
        finalize_add_account(message, user_id)

    # --- СПИСОК АККАУНТОВ ---
    @bot.callback_query_handler(func=lambda c: c.data == "list_accs")
    @auth_required
    def cb_list_accs(call):
        bot.answer_callback_query(call.id)
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT DISTINCT game_name, COUNT(*) FROM accounts GROUP BY game_name ORDER BY COUNT(*) DESC")
        games_data = c.fetchall()
        conn.close()
        
        if not games_data:
            no_games_text = (
                "📭 <b>Нет доступных игр</b>\n\n"
                "🎮 Добавьте первый аккаунт, чтобы начать работу с системой аренды.\n\n"
                "➕ Используйте кнопку \"Добавить новый аккаунт\" в главном меню."
            )
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_acc"))
            kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                                   no_games_text, kb, parse_mode="HTML")
            return
        
        # Формируем список игр для клавиатуры
        games_list = [game for game, count in games_data]
        
        # Создаем текст с подробной информацией
        games_text = "🎮 <b>Управление аккаунтами по играм</b>\n\n"
        games_text += "📊 <b>Доступные категории:</b>\n\n"
        
        for game, count in games_data:
            emoji = get_game_emoji(game)
            # Получаем статистику по свободным аккаунтам
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM accounts WHERE game_name=? AND status='free'", (game,))
            free_count = c.fetchone()[0]
            conn.close()
            
            status_text = f"({free_count}/{count} свободно)"
            games_text += f"{emoji} <b>{game}</b> - {count} аккаунтов {status_text}\n"
        
        games_text += "\n🔍 Выберите игру для управления аккаунтами:"
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               games_text, game_selection_kb(games_list), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("select_game:"))
    @auth_required
    def cb_select_game(call):
        logger.debug(f"[SELECT_GAME] Callback received: {call.data}")
        bot.answer_callback_query(call.id)
        try:
            game = call.data.split(":", 1)[1]
            logger.debug(f"[SELECT_GAME] Game selected: {game}")
            show_accounts_page(call.message, game, 0)
        except Exception as e:
            logger.error(f"[SELECT_GAME] Error handling select_game callback {call.data}: {e}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
            markup.add(types.InlineKeyboardButton("🔄 В главное меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, "Произошла ошибка при выборе игры.", reply_markup=markup)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("page:"))
    @auth_required
    def cb_page_accs(call):
        logger.debug(f"[PAGE_ACCS] Callback received: {call.data}")
        bot.answer_callback_query(call.id)
        try:
            parts = call.data.split(":")
            if len(parts) == 3:
                command, game, index_str = parts
                try:
                    index = int(index_str)
                    logger.debug(f"[PAGE_ACCS] Navigating to game={game}, index={index}")
                    show_accounts_page(call.message, game, index)
                except ValueError:
                    logger.error(f"[PAGE_ACCS] Invalid index in page callback data: {call.data}")
                    bot.answer_callback_query(call.id, "❌ Ошибка навигации: неверный индекс")
            else:
                logger.error(f"[PAGE_ACCS] Invalid page callback data: {call.data}")
                bot.answer_callback_query(call.id, "❌ Ошибка навигации")
        except Exception as e:
            logger.error(f"[PAGE_ACCS] Error handling page_accs callback {call.data}: {e}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
            markup.add(types.InlineKeyboardButton("🔄 В главное меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, "Произошла ошибка при навигации.", reply_markup=markup)

    # Функция для отображения страницы с аккаунтами
    # Принимает message (из call.message), название игры и индекс ПЕРВОГО аккаунта на странице
    def show_accounts_page(message, game, start_index):
        """Показывает страницу с аккаунтами для выбранной игры с пагинацией"""
        logger.debug(f"[SHOW_ACC_PAGE] Showing accounts for game={game}, starting index={start_index}")
        PAGE_SIZE = 7 # Количество аккаунтов на странице
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            # Получаем все аккаунты для игры, отсортированные по ID
            # Убедимся, что получаем steam_guard_enabled
            c.execute("SELECT id, login, password, status, steam_guard_enabled, rented_until FROM accounts WHERE game_name=? ORDER BY id", (game,))
            all_accounts = c.fetchall()
            conn.close()

            total_accounts = len(all_accounts)
            logger.debug(f"[SHOW_ACC_PAGE] Found {total_accounts} total accounts for game {game}")

            if total_accounts == 0:
                logger.debug(f"[SHOW_ACC_PAGE] No accounts found for game {game}")
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
                markup.add(types.InlineKeyboardButton("🔄 В главное меню", callback_data="back_to_menu"))
                safe_edit_message_text(bot, message.chat.id, message.message_id, "Нет аккаунтов для этой игры.", reply_markup=markup)
                return

            # Определяем аккаунты для текущей страницы
            accounts_on_page = all_accounts[start_index : start_index + PAGE_SIZE]
            current_page_count = len(accounts_on_page)
            logger.debug(f"[SHOW_ACC_PAGE] Showing {current_page_count} accounts from index {start_index}")

            # Формируем заголовок с информацией о странице
            end_index = start_index + current_page_count
            total_free = sum(1 for acc in all_accounts if acc[3] == 'free')
            total_rented = len(all_accounts) - total_free
            
            text = f"<b>🎮 {game} — Аккаунты</b>\n"
            text += f"📊 Всего: {len(all_accounts)} | 🟢 Свободно: {total_free} | 🔴 В аренде: {total_rented}\n"
            text += f"📄 Страница: {start_index // PAGE_SIZE + 1} из {(len(all_accounts) + PAGE_SIZE - 1) // PAGE_SIZE}\n\n"

            # Создаем inline кнопки для каждого аккаунта на странице
            markup = types.InlineKeyboardMarkup()
            for acc_id, login, password, status, steam_guard_enabled, rented_until_timestamp in accounts_on_page:
                # Создаем эмодзи статуса
                if status == 'free':
                    status_emoji = '🟢'
                    status_text = 'Свободен'
                else:
                    status_emoji = '🔴'
                    status_text = 'В аренде'
                
                # Добавляем информацию о Steam Guard
                guard_emoji = '🔒' if steam_guard_enabled else '🔓'
                
                # Красивое имя кнопки с эмодзи
                button_text = f"{status_emoji} {login} {guard_emoji}"

                # Добавляем информацию об аренде, если аккаунт в аренде
                if status == "rented" and rented_until_timestamp:
                    try:
                        # Преобразуем timestamp в datetime объект
                        rented_until_dt = datetime.fromtimestamp(rented_until_timestamp)
                        now = datetime.now()
                        remaining_time = rented_until_dt - now

                        # Форматируем время окончания (локальное время сервера, для МСК нужна доп. библа)
                        end_time_str = rented_until_dt.strftime('%H:%M:%S %d.%m.%Y') # Формат с датой тоже

                        # Форматируем оставшееся время
                        total_seconds = int(remaining_time.total_seconds())
                        if total_seconds < 0:
                            remaining_str = "Время аренды истекло"
                        else:
                            hours, remainder = divmod(total_seconds, 3600)
                            minutes, seconds = divmod(remainder, 60)
                            remaining_parts = []
                            if hours > 0: remaining_parts.append(f"{hours} ч.")
                            if minutes > 0: remaining_parts.append(f"{minutes} мин.")
                            remaining_parts.append(f"{seconds} сек.") # Секунды всегда показываем
                            remaining_str = ", ".join(remaining_parts)
                            if not remaining_str: remaining_str = "< 1 сек."

                        # Добавляем строки в текст сообщения
                        button_text += f" до {end_time_str} (осталось: {remaining_str})"

                    except Exception as e:
                        logger.error(f"Ошибка при расчете или форматировании времени аренды в show_accounts_page: {e}")
                        button_text += " (Ошибка времени)"

                # Callback Data для перехода на страницу полной информации об аккаунте
                markup.add(types.InlineKeyboardButton(button_text, callback_data=f"info:{acc_id}"))

            # Добавляем навигационные кнопки Пред/След, если нужно
            nav_buttons = []
            # Кнопка "Пред" если это не первая страница
            if start_index > 0:
                nav_buttons.append(types.InlineKeyboardButton("⬅️ Пред", callback_data=f"page:{game}:{start_index - PAGE_SIZE}"))
            # Кнопка "След" если есть еще аккаунты после текущей страницы
            if start_index + PAGE_SIZE < total_accounts:
                nav_buttons.append(types.InlineKeyboardButton("След ➡️", callback_data=f"page:{game}:{start_index + PAGE_SIZE}"))

            # Добавляем навигационную строку, если есть кнопки навигации
            if nav_buttons:
                markup.row(*nav_buttons)

            # Добавляем кнопку "Назад к списку игр"
            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))

            # Обновляем сообщение
            safe_edit_message_text(bot, message.chat.id, message.message_id, text, reply_markup=markup, parse_mode='HTML')

        except Exception as e:
            logger.error(f"[SHOW_ACC_PAGE] Ошибка при отображении страницы аккаунтов: {e}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
            markup.add(types.InlineKeyboardButton("🔄 В главное меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, message.chat.id, message.message_id, "Произошла ошибка при отображении списка аккаунтов.", reply_markup=markup)

    # --- ИНФОРМАЦИЯ ОБ АККАУНТЕ ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("info:"))
    @auth_required
    def cb_info(call):
        bot.answer_callback_query(call.id)
        try:
            acc_id = call.data.split(":", 1)[1]
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT id, login, password, game_name, status, steam_guard_enabled, rented_until, email_login, email_password FROM accounts WHERE id=?", (acc_id,))
            row = c.fetchone()
            conn.close()

            if not row:
                bot.answer_callback_query(call.id, "❌ Аккаунт не найден")
                return

            acc_id, login, password, game_name, status, steam_guard_enabled, rented_until_timestamp, email_login, email_password = row

            status_text = "🟢 Свободен" if status == "free" else "🔴 В аренде"
            text = f"<b>Аккаунт:</b> <code>{html.escape(login)}</code>\n"
            text += f"<b>Пароль:</b> <code>{html.escape(password)}</code>\n"
            text += f"<b>Статус:</b> {status_text}\n"
            text += f"<b>Игра:</b> <code>{html.escape(game_name)}</code>"

            # Добавляем информацию о почте
            if email_login:
                text += f"\n<b>Почта:</b> <code>{html.escape(email_login)}</code>"
            if email_password:
                text += f"\n<b>Пароль от почты:</b> <code>{html.escape(email_password)}</code>"

            # Добавляем информацию об аренде, если аккаунт в аренде
            if status == "rented" and rented_until_timestamp:
                try:
                    # Преобразуем timestamp в datetime объект
                    rented_until_dt = datetime.fromtimestamp(rented_until_timestamp)
                    now = datetime.now()
                    remaining_time = rented_until_dt - now

                    # Форматируем время окончания (локальное время сервера, для МСК нужна доп. библ.)
                    end_time_str = rented_until_dt.strftime('%H:%M:%S %d.%m.%Y') # Формат с датой тоже

                    # Форматируем оставшееся время
                    total_seconds = int(remaining_time.total_seconds())
                    if total_seconds < 0:
                        remaining_str = "Время аренды истекло"
                    else:
                        hours, remainder = divmod(total_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        remaining_parts = []
                        if hours > 0: remaining_parts.append(f"{hours} ч.")
                        if minutes > 0: remaining_parts.append(f"{minutes} мин.")
                        remaining_parts.append(f"{seconds} сек.") # Секунды всегда показываем
                        remaining_str = ", ".join(remaining_parts)
                        if not remaining_str: remaining_str = "< 1 сек."

                    # Добавляем строки в текст сообщения
                    text += f"\nАрендован до (локальное время сервера): {html.escape(end_time_str)}"
                    text += f"\nОсталось: {html.escape(remaining_str)}"

                except Exception as e:
                    logger.error(f"Ошибка при расчете или форматировании времени аренды: {e}")
                    text += "\nОшибка при расчете времени аренды."

            markup = types.InlineKeyboardMarkup(row_width=2)
            
            # Первая строка - основные действия с аккаунтом
            if status == "free":
                markup.row(
                    types.InlineKeyboardButton("🟢 Арендовать", callback_data=f"rent:{acc_id}"),
                    types.InlineKeyboardButton("🧪 Тест", callback_data=f"test:{acc_id}")
                )
            else:
                markup.add(types.InlineKeyboardButton("⏹ Завершить аренду", callback_data=f"return:{acc_id}"))
                markup.add(types.InlineKeyboardButton("🧪 Тест", callback_data=f"test:{acc_id}"))
            
            # Вторая строка - настройки и управление
            logger.debug(f"[CB_INFO] game_name from DB: '{game_name}'")
            guard_button_text = "🟢 Искать код" if steam_guard_enabled else "🔴 Не искать код"
            toggle_guard_callback_data = f"toggle_guard:{acc_id}:{game_name}"
            login_settings_callback = f"login_settings:{acc_id}"
            
            markup.row(
                types.InlineKeyboardButton("📝 Сменить данные", callback_data=f"chgdata:{acc_id}"),
                types.InlineKeyboardButton("⚙️ Настройки входа", callback_data=login_settings_callback)
            )
            
            # Третья строка - Steam Guard и дополнительные функции
            markup.row(
                types.InlineKeyboardButton(guard_button_text, callback_data=toggle_guard_callback_data),
                types.InlineKeyboardButton("📮 Получить код", callback_data=f"get_code:{acc_id}")
            )
            
            # Четвертая строка - удаление (только для свободных) и навигация
            if status == "free":
                markup.row(
                    types.InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{acc_id}"),
                    types.InlineKeyboardButton("◀️ Назад", callback_data=f"select_game:{game_name}")
                )
            else:
                markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"select_game:{game_name}"))

            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, reply_markup=markup, parse_mode="HTML")

        except Exception as e:
            logger.error(f"[CB_INFO] Ошибка при отображении информации об аккаунте {{call.data}}: {e}")
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, "Произошла ошибка при получении информации об аккаунте.", reply_markup=markup, parse_mode="HTML")

    # --- АРЕНДА АККАУНТА (ВЫБОР ВРЕМЕНИ) ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent:"))
    @auth_required
    def cb_rent(call):
        bot.answer_callback_query(call.id)
        try:
            acc_id = call.data.split(":")[1]
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT * FROM accounts WHERE id=?", (acc_id,))
            acc = c.fetchone()
            conn.close()

            if not acc:
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
                return

            if acc[5] != "free":
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, "❌ Аккаунт уже в аренде.")
                return
            
            # Показываем варианты времени аренды
            text = f"⏰ Выберите время аренды для аккаунта <code>{html.escape(acc[1])}</code>:"
            markup = types.InlineKeyboardMarkup()
            
            # Стандартные варианты времени
            time_options = [
                ("1 час", 1),
                ("3 часа", 3),
                ("6 часов", 6),
                ("12 часов", 12),
                ("24 часа", 24)
            ]
            
            for text_option, hours in time_options:
                markup.add(types.InlineKeyboardButton(
                    text_option, 
                    callback_data=f"rent_time:{acc_id}:{hours}"
                ))
            
            # Кнопка для ввода кастомного времени
            markup.add(types.InlineKeyboardButton(
                "⚙️ Кастомное время", 
                callback_data=f"rent_custom:{acc_id}"
            ))
            
            # Кнопка назад
            markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"info:{acc_id}"))
            
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, reply_markup=markup, parse_mode="HTML")

        except Exception as e:
            logger.error(f"Ошибка в cb_rent: {e}")
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text("Произошла ошибка при выборе времени аренды.", call.message.chat.id, call.message.message_id, reply_markup=main_menu())
            except Exception:
                bot.send_message(call.message.chat.id, "Произошла ошибка при выборе времени аренды.", reply_markup=main_menu())

    # --- АРЕНДА АККАУНТА С УКАЗАННЫМ ВРЕМЕНЕМ ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent_time:"))
    @auth_required
    def cb_rent_time(call):
        bot.answer_callback_query(call.id)
        try:
            parts = call.data.split(":")
            acc_id = parts[1]
            hours = int(parts[2])
            
            # Выполняем аренду
            execute_rent(call, acc_id, hours)

        except Exception as e:
            logger.error(f"Ошибка в cb_rent_time: {e}")
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text("Произошла ошибка при аренде аккаунта.", call.message.chat.id, call.message.message_id, reply_markup=main_menu())
            except Exception:
                bot.send_message(call.message.chat.id, "Произошла ошибка при аренде аккаунта.", reply_markup=main_menu())

    # --- КАСТОМНАЯ АРЕНДА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent_custom:"))
    @auth_required
    def cb_rent_custom(call):
        bot.answer_callback_query(call.id)
        try:
            acc_id = call.data.split(":")[1]
            
            # Запрашиваем кастомное время
            text = "⏰ Введите время аренды в часах (например: 5 или 0.5 для 30 минут):"
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("◀️ Назад к выбору времени", callback_data=f"rent:{acc_id}"))
            
            # Сохраняем состояние ожидания ввода времени
            user_states[call.from_user.id] = {
                'state': 'awaiting_custom_rent_time',
                'account_id': acc_id,
                'chat_id': call.message.chat.id,
                'message_id': call.message.message_id
            }
            
            safe_edit_message_text(bot, text, call.message.chat.id, call.message.message_id, reply_markup=markup)

        except Exception as e:
            logger.error(f"Ошибка в cb_rent_custom: {e}")
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text("Произошла ошибка при настройке кастомной аренды.", call.message.chat.id, call.message.message_id, reply_markup=main_menu())
            except Exception:
                bot.send_message(call.message.chat.id, "Произошла ошибка при настройке кастомной аренды.", reply_markup=main_menu())

    # --- ФУНКЦИЯ ВЫПОЛНЕНИЯ АРЕНДЫ ---
    def execute_rent(call, acc_id, hours):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT * FROM accounts WHERE id=?", (acc_id,))
            acc = c.fetchone()
            conn.close()

            if not acc:
                bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
                return

            if acc[5] != "free":
                bot.send_message(call.message.chat.id, "❌ Аккаунт уже в аренде.")
                return
                
            # Вычисляем время аренды
            rent_seconds = int(hours * 60 * 60)
            rented_until = datetime.now() + timedelta(seconds=rent_seconds)
            
            try:
                # Для аренды через телеграм используем ID заказа с префиксом TG-
                tg_order_id = f"TG-{acc_id}"
                mark_account_rented(acc_id, call.from_user.id, rented_until.timestamp(), tg_order_id)
            except Exception as e:
                logger.error(f"Ошибка при маркировке аккаунта как арендованного: {e}")
                bot.send_message(call.message.chat.id, "❌ Ошибка при аренде аккаунта.")
                return

            # Отправляем данные аккаунта пользователю
            order = {'chat_id': call.message.chat.id, 'buyer': call.from_user.id, 'description': acc[3]}
            
            try:
                pass  # если нужно, оставим вызов для FunPay отдельно NOTE next update~~~
            except Exception as e:
                logger.error(f"Ошибка при отправке данных аккаунта: {e}")
                
            # Запускаем авто-возврат
            def notify_callback(acc_id, tg_user_id):
                try:
                    # Создаем объект заказа с правильным order_id
                    from steam.steam_account_rental_utils import send_order_completed_message, DB_PATH
                    import sqlite3
                    
                    # Получаем order_id из базы данных
                    order_id = None
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute("PRAGMA table_info(accounts)")
                        columns = [column[1] for column in c.fetchall()]
                        
                        if 'order_id' in columns:
                            c.execute("SELECT order_id FROM accounts WHERE id=?", (acc_id,))
                            row = c.fetchone()
                            if row and row[0]:
                                order_id = row[0]
                        conn.close()
                    except Exception as e:
                        logger.error(f"Ошибка при получении order_id из БД: {e}")
                    
                    # Используем полученный order_id или формируем TG-префикс
                    real_order_id = order_id or f"TG-{acc_id}"
                    order_data = {'chat_id': tg_user_id, 'order_id': real_order_id}
                    
                    # Отправляем завершающее сообщение
                    send_order_completed_message(order_data, bot.send_message)
                except Exception as e:
                    logger.error(f"Ошибка в callback для аренды: {e}")
                    
            try:
                auto_end_rent(acc_id, call.from_user.id, rent_seconds, notify_callback=notify_callback)
            except Exception as e:
                logger.error(f"Ошибка при настройке автовозврата: {e}")
                
            bot.answer_callback_query(call.id)
            
            # Формируем подробное сообщение об аренде
            rented_until_str = rented_until.strftime('%H:%M:%S %d.%m.%Y')
            remaining_time = rented_until - datetime.now()
            total_seconds = int(remaining_time.total_seconds())
            if total_seconds < 0:
                remaining_str = "Время аренды истекло"
            else:
                hours, remainder = divmod(total_seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                remaining_parts = []
                if hours > 0: remaining_parts.append(f"{hours} ч.")
                if minutes > 0: remaining_parts.append(f"{minutes} мин.")
                remaining_parts.append(f"{seconds} сек.")
                remaining_str = ", ".join(remaining_parts)
                if not remaining_str: remaining_str = "< 1 сек."
            
            # Форматируем время аренды для сообщения
            if hours == int(hours):
                hours_text = f"{int(hours)} часов"
            else:
                hours_text = f"{hours} часов"
                
            rent_msg = (
                f"✅ Аккаунт #{acc[0]} ({acc[1]}) арендован на {hours_text}!\n"
                f"Аренда закончится: {rented_until_str}\n"
                f"Осталось: {remaining_str}"
            )
            success_msg = bot.send_message(call.message.chat.id, rent_msg)
            
            def delete_rent_message():
                time.sleep(6)
                try:
                    bot.delete_message(call.message.chat.id, success_msg.message_id)
                except Exception as e:
                    logger.error(f"Ошибка при удалении сообщения об аренде: {e}")

            threading.Thread(target=delete_rent_message, daemon=True).start()

            # Обновляем карточку аккаунта
            cb_info(call)

        except Exception as e:
            logger.error(f"Неожиданная ошибка в cb_rent: {e}")
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text("Произошла ошибка при аренде аккаунта.", call.message.chat.id, call.message.message_id, reply_markup=main_menu())
            except Exception:
                bot.send_message(call.message.chat.id, "Произошла ошибка при аренде аккаунта.", reply_markup=main_menu())

    # --- ВОЗВРАТ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("return:"))
    @auth_required
    def cb_return(call):
        bot.answer_callback_query(call.id)
        try:
            acc_id = call.data.split(":")[1]
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            # Проверяем наличие столбца order_id
            c.execute("PRAGMA table_info(accounts)")
            columns = [column[1] for column in c.fetchall()]
            order_id = None
            tg_user_id = None
            if 'order_id' in columns:
                c.execute("SELECT status, order_id, tg_user_id FROM accounts WHERE id=?", (acc_id,))
                row = c.fetchone()
                if row:
                    order_id = row[1]
                    tg_user_id = row[2]
            else:
                c.execute("SELECT status FROM accounts WHERE id=?", (acc_id,))
                row = c.fetchone()
            conn.close()

            if not row:
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
                return
            if row[0] != "rented":
                bot.answer_callback_query(call.id)
                bot.send_message(call.message.chat.id, "❌ Аккаунт не в аренде.")
                return
            try:
                mark_account_free(acc_id)
                # --- Новая логика уведомлений ---
                funpay_msg = (
                    "Ваша аренда была завершена администратором.\n"
                    "Возможная причина — нарушение условий использования аккаунта или иная внутренняя причина.\n"
                    "Если у вас есть вопросы, пожалуйста, свяжитесь с продавцом для уточнения деталей."
                )
                admin_msg = (
                    "АРЕНДА ЗАВЕРШЕНА (аккаунт освобождён для новых аренд).\n"
                    "Клиент всё ещё в аккаунте — для полного сброса доступа используйте функцию 'Сменить данные'."
                )
                # Если есть tg_user_id и order_id — отправить клиенту в FunPay
                if tg_user_id and order_id and str(tg_user_id).isdigit() and not str(order_id).startswith('TG-'):
                    try:
                        from funpay_integration import FunPayListener
                        funpay = FunPayListener()
                        funpay.funpay_send_message_wrapper(tg_user_id, funpay_msg)
                    except Exception as e:
                        logger.error(f"Ошибка при отправке сообщения клиенту FunPay: {e}")
                # Сообщение админу в Telegram
                msg = bot.send_message(call.message.chat.id, admin_msg)
                bot.answer_callback_query(call.id)

                def delete_admin_msg():
                        import time
                        time.sleep(5)
                        try:
                            bot.delete_message(call.message.chat.id, msg.message_id)
                        except Exception as e:
                            logger.error(f"Ошибка при удалении сообщения о завершении аренды: {e}")
                import threading
                threading.Thread(target=delete_admin_msg, daemon=True).start()

                # Получаем название игры для возврата
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT game_name FROM accounts WHERE id=?", (acc_id,))
                game_row = c.fetchone()
                conn.close()
                game_name = game_row[0] if game_row else None

                # Обновляем карточку аккаунта
                cb_info(call)

            except Exception as e:
                logger.error(f"Ошибка при освобождении аккаунта: {e}")
                bot.answer_callback_query(call.id, "Ошибка при завершении аренды", show_alert=True)
                safe_edit_message_text(bot, call.message.chat.id, call.message.id, "Произошла ошибка при завершении аренды.", reply_markup=main_menu())
        except Exception as e:
            logger.error(f"Неожиданная ошибка в cb_return: {e}")
            bot.answer_callback_query(call.id)
            safe_edit_message_text(bot, call.message.chat.id, call.message.id, "Произошла ошибка при завершении аренды.", reply_markup=main_menu())

    # --- УДАЛЕНИЕ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("del:"))
    @auth_required
    def cb_del(call):
        acc_id = call.data.split(":")[1]
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT login FROM accounts WHERE id=?", (acc_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            bot.answer_callback_query(call.id, "❌ Аккаунт не найден")
            return
        
        login = row[0]
        c.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
        conn.commit()
        conn.close()
        
        text = f"✅ Аккаунт <b>{login}</b> удален"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("◀️ В меню", callback_data="back_to_menu"))
        
        safe_edit_message_text(bot, text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="HTML")

    # --- ПОЛУЧЕНИЕ КОДА GUARD ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("guard:"))
    @auth_required
    def cb_get_guard(call):
        acc_id = call.data.split(":")[1]
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT email_login, email_password, imap_host FROM accounts WHERE id=?", (acc_id,))
        row = c.fetchone()
        conn.close()

        if not row or not all(row):
            bot.answer_callback_query(call.id, "❌ Нет данных почты")
            return

        email_login, email_password, imap_host = row
        host, port = parse_imap_host_port(imap_host)

        def get_guard_code():
            try:
                code = fetch_steam_guard_code_from_email(email_login, email_password, host, port)
                if code:
                    bot.send_message(call.message.chat.id, f"🔑 Код Guard: <code>{code}</code>", parse_mode="HTML")
                else:
                    bot.send_message(call.message.chat.id, "❌ Код не найден")
            except Exception as e:
                logger.error(f"Ошибка при получении кода: {e}")
                bot.send_message(call.message.chat.id, f"❌ Ошибка: {e}")
                
        threading.Thread(target=get_guard_code).start()
        bot.answer_callback_query(call.id, "⏳ Получаем код...")

    # --- ВЫХОД ИЗ АККАУНТА ---
    @bot.message_handler(commands=['logout'])
    def cmd_self_logout(message):
        user_id = message.from_user.id
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL WHERE tg_user_id=?", (user_id,))
        conn.commit()
        conn.close()
        bot.send_message(message.chat.id, "✅ Вы вышли из всех аккаунтов")

    # --- ВЫХОД ДРУГОГО ПОЛЬЗОВАТЕЛЯ (только для админов) ---
    @bot.message_handler(commands=['logout_user'])
    @auth_required
    def cmd_logout_user(message):
        if message.from_user.id not in ADMIN_IDS:
            bot.send_message(message.chat.id, "❌ У вас нет прав для этой команды")
            return

        try:
            user_id = int(message.text.split()[1])
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL WHERE tg_user_id=?", (user_id,))
            conn.commit()
            conn.close()
            bot.send_message(message.chat.id, f"✅ Пользователь {user_id} выведен из всех аккаунтов")
        except (IndexError, ValueError):
            bot.send_message(message.chat.id, "❌ Укажите ID пользователя")

    # --- ОБРАБОТКА НЕИЗВЕСТНЫХ КОМАНД ---
    @bot.message_handler(func=lambda m: True)
    def fallback(message):
        user_id = message.from_user.id
        logger.info(f"[FALLBACK] Received message from user {user_id}: {message.text}")
        logger.info(f"[FALLBACK] Current user states: {user_states}")
        logger.info(f"[FALLBACK] User state: {user_states.get(user_id)}")
        
        # Проверяем, есть ли у пользователя состояние awaiting_input
        if user_id in user_states and user_states[user_id]['state'].startswith('awaiting_input_'):
            logger.info(f"[FALLBACK] User {user_id} has awaiting_input state, calling handle_awaiting_input")
            handle_awaiting_input(message)
            return
            
        # Проверяем, есть ли у пользователя состояние awaiting_custom_rent_time
        if user_id in user_states and user_states[user_id].get('state') == 'awaiting_custom_rent_time':
            logger.info(f"[FALLBACK] User {user_id} has awaiting_custom_rent_time state, calling handle_custom_rent_time")
            handle_custom_rent_time(message)
            return
            
        bot.send_message(message.chat.id, "Неизвестная команда. Используйте /menu для доступа к основному меню.")

    # --- ПЕРЕКЛЮЧЕНИЕ STEAM GUARD ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("toggle_guard:"))
    @auth_required
    def cb_toggle_guard(call):
        logger.debug(f"[TOGGLE] cb_toggle_guard вызван с данными: {call.data}")
        bot.answer_callback_query(call.id, "Обработка...", show_alert=False)
        try:
            # Парсим данные из callback_data
            parts = call.data.split(":", 2)  # Разделяем только на 3 части
            if len(parts) != 3:
                logger.error(f"[TOGGLE] Неверный формат callback_data: {call.data}")
                return
                
            acc_id = parts[1]
            game_name = parts[2]  # Оставшаяся часть - это название игры
            
            logger.debug(f"[TOGGLE] Переключение Steam Guard для аккаунта {acc_id}")
            
            # Подключаемся к БД
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Получаем текущее состояние
            c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc_id,))
            row = c.fetchone()
            
            if not row:
                logger.error(f"[TOGGLE] Аккаунт {acc_id} не найден")
                bot.answer_callback_query(call.id, "Аккаунт не найден", show_alert=True)
                conn.close()
                return
                
            # Безопасно преобразуем в число
            try:
                current_state = int(row[0]) if row[0] is not None else 1
            except (ValueError, TypeError):
                logger.warning(f"[TOGGLE] Некорректное значение в БД: {row[0]}, используем значение по умолчанию 1")
                current_state = 1
                
            # Инвертируем состояние
            new_state = 0 if current_state else 1
            
            logger.debug(f"[TOGGLE] Текущее состояние: {current_state}, новое состояние: {new_state}")
            
            # Обновляем в БД
            c.execute("UPDATE accounts SET steam_guard_enabled = ? WHERE id=?", (new_state, acc_id))
            conn.commit()
            
            # Проверяем, что обновление прошло успешно
            c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc_id,))
            updated_row = c.fetchone()
            
            if updated_row:
                try:
                    updated_state = int(updated_row[0]) if updated_row[0] is not None else 1
                except (ValueError, TypeError):
                    logger.error(f"[TOGGLE] Некорректное значение после обновления: {updated_row[0]}")
                    updated_state = new_state
            else:
                logger.error("[TOGGLE] Не удалось получить обновленное значение")
                updated_state = new_state
                
            conn.close()
            
            if updated_state == new_state:
                logger.debug(f"[TOGGLE] Состояние успешно обновлено в БД")
                # Отвечаем на callback
                bot.answer_callback_query(call.id, f"Настройка обновлена! {'Включен' if new_state else 'Выключен'} поиск кода для клиента", show_alert=True)
                
                # Обновляем страницу со списком аккаунтов
                show_accounts_page(call.message, game_name, 0)
                
            else:
                logger.error(f"[TOGGLE] Ошибка обновления состояния в БД. Ожидалось: {new_state}, получено: {updated_state}")
                bot.answer_callback_query(call.id, "Ошибка обновления настройки", show_alert=True)
            
        except Exception as e:
            logger.error(f"[TOGGLE] Ошибка при переключении Steam Guard: {e}")
            bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)

    # --- НАСТРОИТЬ ПОЧТУ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("mail:"))
    @auth_required
    def cb_mail(call):
        try:
            acc_id = call.data.split(":")[1]
            user_id = call.from_user.id

            if user_id not in user_acc_data:
                user_acc_data[user_id] = {}
            user_acc_data[user_id]['chg_acc_id'] = acc_id
            user_states[user_id] = "change_mail_login"

            bot.answer_callback_query(call.id, "✏️ Введите новый логин почты:")
            bot.delete_message(call.message.chat.id, call.message.message_id)

        except Exception as e:
            logger.error(f"Error handling mail callback {call.data}: {e}")
            bot.answer_callback_query(call.id, "❌ Произошла ошибка при настройке почты")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_login")
    @auth_required
    def change_mail_login_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_mail_login = message.text.strip()

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET email_login=? WHERE id=?", (new_mail_login, acc_id))
            conn.commit()
            conn.close()

            user_acc_data[user_id]['new_mail_login'] = new_mail_login
            user_states[user_id] = "change_mail_password"
            bot.send_message(message.chat.id, "✏️ Введите новый пароль почты:")

        except Exception as e:
            logger.error(f"Error changing mail login for account {acc_id}: {e}")
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене логина почты.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_password")
    @auth_required
    def change_mail_password_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_mail_password = message.text.strip()

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET email_password=? WHERE id=?", (new_mail_password, acc_id))
            conn.commit()
            conn.close()

            user_acc_data[user_id]['new_mail_password'] = new_mail_password
            user_states[user_id] = "change_mail_imap"
            bot.send_message(message.chat.id, "✏️ Введите новый IMAP-сервер (например, imap.gmail.com):")

        except Exception as e:
            logger.error(f"Error changing mail password for account {acc_id}: {e}")
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене пароля почты.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_imap")
    @auth_required
    def change_mail_imap_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_imap_host_str = message.text.strip()

        try:
            host, port = parse_imap_host_port(new_imap_host_str)
            
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET imap_host=?, imap_port=? WHERE id=?", (host, port, acc_id))
            conn.commit()
            conn.close()

            updated_mail_login = user_acc_data[user_id].get('new_mail_login', 'Неизвестно')
            
            bot.send_message(message.chat.id, 
                             f"✅ Данные почты для аккаунта {acc_id} успешно обновлены:\n" \
                             f"Логин почты: {updated_mail_login}\n" \
                             f"IMAP-сервер: {host}{f':{port}' if port else ''}", 
                             reply_markup=main_menu())

            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

        except Exception as e:
            logger.error(f"Error changing mail IMAP for account {acc_id}: {e}")
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене IMAP-сервера.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.callback_query_handler(func=lambda call: call.data.startswith('login_settings:'))
    def cb_login_settings(call=None, chat_id=None, message_id=None, account_id=None):
        try:
            # Определяем chat_id, message_id и account_id в зависимости от способа вызова
            if call:
                logger.info(f"[CALLBACK] Received callback: {call.data}")
                current_chat_id = call.message.chat.id
                current_message_id = call.message.message_id
                account_id = call.data.split(':')[1]  # id теперь всегда строка
                # Мы отвечаем на callback здесь, если функция вызвана через callback
                bot.answer_callback_query(call.id, text="Настройки входа")
            elif chat_id is not None and message_id is not None and account_id is not None:
                current_chat_id = chat_id
                current_message_id = message_id
                # account_id уже передан
                logger.info(f"[CALLBACK] Called cb_login_settings programmatically for account {account_id} at chat_id {chat_id}, message_id {message_id}")
                # Здесь мы не отвечаем на callback, так как его обработал вызвавший код (например, cb_cancel_input)
            else:
                logger.error("[CALLBACK] cb_login_settings called with insufficient arguments.")
                # Недостаточно аргументов для работы, нет chat_id для отправки сообщения об ошибке
                return

            # Здесь нужно получить game_name для кнопки Назад
            # В идеале, эти данные должны быть частью callback_data или извлечены из БД по account_id
            # Для простоты сейчас извлеку из БД
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT game_name FROM accounts WHERE id = ?', (account_id,))
            result = cursor.fetchone()
            conn.close()

            game_name = result[0] if result else "Unknown Game"

            keyboard = types.InlineKeyboardMarkup()

            # Кнопки для изменения данных входа
            # Callback data будет содержать account_id и тип поля для изменения
            keyboard.add(types.InlineKeyboardButton("🔑 Логин Steam", callback_data=f"change_field:{account_id}:login"))
            keyboard.add(types.InlineKeyboardButton("🔐 Пароль Steam", callback_data=f"change_field:{account_id}:password"))
            keyboard.add(types.InlineKeyboardButton("📧 Почта", callback_data=f"change_field:{account_id}:email_login"))
            keyboard.add(types.InlineKeyboardButton("🔒 Пароль от почты", callback_data=f"change_field:{account_id}:email_password"))
            keyboard.add(types.InlineKeyboardButton("🌐 IMAP сервер", callback_data=f"change_field:{account_id}:imap_host"))

            # Кнопка Назад в главное меню настроек аккаунта
            # Важно: здесь callback_data должен вести обратно к info:{account_id}
            back_to_info_callback = f"info:{account_id}"
            keyboard.add(types.InlineKeyboardButton("◀️ Назад", callback_data=back_to_info_callback))

            text = f"⚙️ Настройки входа для аккаунта #{account_id}"

            try:
                # Пробуем отредактировать существующее сообщение
                bot.edit_message_text(
                    chat_id=current_chat_id,
                    message_id=current_message_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            except Exception as edit_error:
                logger.warning(f"Failed to edit message, sending new one: {edit_error}")
                # Если не удалось отредактировать, отправляем новое сообщение
                bot.send_message(
                    chat_id=current_chat_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )

            if call:
                bot.answer_callback_query(call.id, text="Настройки входа")

        except Exception as e:
            logger.error(f"Error in cb_login_settings: {e}", exc_info=True)
            # Обработка ошибок: проверяем, как была вызвана функция
            if call:
                # Если вызвана через callback, отвечаем на callback query и отправляем сообщение
                 try:
                     bot.answer_callback_query(call.id, text="Произошла ошибка.", show_alert=True)
                     # Отправляем сообщение об ошибке в чат, используя данные из call
                     bot.send_message(call.message.chat.id, f"❌ Произошла ошибка при загрузке настроек входа: {e}")
                 except Exception as answer_e:
                     logger.error(f"Failed to answer callback query or send message in cb_login_settings error handler (from call): {answer_e}")
            elif chat_id:
                # Если вызвана программно (из cb_cancel_input) и chat_id доступен, отправляем сообщение об ошибке
                bot.send_message(chat_id, f"❌ Произошла ошибка при загрузке настроек входа: {e}")
            else:
                 # Если chat_id тоже недоступен, просто логируем
                 logger.error("Error in cb_login_settings, chat_id not available to send message.")

    # --- Обработчики для изменения данных входа ---
    @bot.message_handler(func=lambda message: message.from_user.id in user_states and user_states[message.from_user.id]['state'].startswith('awaiting_input_'))
    def handle_awaiting_input(message):
        user_id = message.from_user.id
        logger.info(f"[INPUT] Received message from user {user_id}: {message.text}")
        logger.info(f"[INPUT] Current user states: {user_states}")
        
        state_info = user_states.get(user_id)
        logger.info(f"[INPUT] State info for user {user_id}: {state_info}")

        if not state_info:
            logger.error(f"[INPUT] No state found for user {user_id}")
            bot.send_message(message.chat.id, "Произошла внутренняя ошибка. Пожалуйста, попробуйте снова.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        state = state_info['state']
        account_id = state_info['account_id']
        field_to_change = state.replace('awaiting_input_', '')
        new_value = message.text

        logger.info(f"[INPUT] Processing input for field {field_to_change} of account {account_id}")
        logger.info(f"[INPUT] New value: {new_value}")

        # Проверка на кнопку "Отмена"
        if new_value == "Отмена":
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            # Возвращаемся в меню настроек входа
            # Нужно вызвать cb_login_settings для текущего аккаунта
            # Имитируем объект call для вызова cb_login_settings
            # Создаем фиктивный объект Call с необходимыми атрибутами, используя данные из state_info
            class MockCall:
                def __init__(self, message_id, chat_id, user_id, data):
                    self.id = f"mock_{message_id}" # Уникальный ID для callback
                    self.message = type('MockMessage', (object,), {'message_id': message_id, 'chat': type('MockChat', (object,), {'id': chat_id})})()
                    self.from_user = type('MockUser', (object,), {'id': user_id})()
                    self.data = data

            mock_call = MockCall(message.message_id, message.chat.id, user_id, f"login_settings:{account_id}") # Используем message.message_id и chat.id

            # Удаляем предыдущее сообщение бота с запросом перед возвратом
            try:
                 bot.delete_message(message.chat.id, state_info['request_message_id'])
            except Exception as e:
                 logger.error(f"Error deleting request message on cancel: {e}")

            cb_login_settings(mock_call)
            return

        # Удаляем предыдущее сообщение с запросом ввода и сообщение пользователя
        try:
            bot.delete_message(message.chat.id, message.message_id) # Удаляем сообщение пользователя с новыми данными
            bot.delete_message(message.chat.id, state_info['request_message_id']) # Удаляем сообщение бота с запросом
        except Exception as e:
            logger.error(f"Error deleting messages: {e}")

        # --- Логика изменения данных в БД ---
        conn = None
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # Проверяем, существует ли такое поле в таблице accounts
            cursor.execute(f"PRAGMA table_info(accounts)")
            columns = [column[1] for column in cursor.fetchall()]

            if field_to_change in columns:
                # Получаем старое значение перед обновлением
                cursor.execute(f"SELECT {field_to_change} FROM accounts WHERE id = ?", (account_id,))
                result = cursor.fetchone()
                if not result:
                    raise Exception(f"Аккаунт #{account_id} не найден в базе данных")
                old_value = result[0]

                # Обновляем поле в базе данных
                update_query = f"UPDATE accounts SET {field_to_change} = ? WHERE id = ?"
                cursor.execute(update_query, (new_value, account_id))
                conn.commit()
                logger.info(f"[DB] Account {account_id}: updated field {field_to_change} from '{old_value}' to '{new_value}'")

                # Обновляем данные аккаунта в user_acc_data в памяти, если они там есть
                if user_id in user_acc_data and user_acc_data[user_id] and user_acc_data[user_id]['id'] == account_id:
                     if field_to_change == 'steam_login':
                         user_acc_data[user_id]['login'] = new_value
                     elif field_to_change == 'steam_password':
                         user_acc_data[user_id]['password'] = new_value
                     elif field_to_change == 'email_login':
                         user_acc_data[user_id]['email_login'] = new_value
                     elif field_to_change == 'email_password':
                         user_acc_data[user_id]['email_password'] = new_value
                     elif field_to_change == 'imap_host':
                         user_acc_data[user_id]['imap_host'] = new_value
                         # Если меняем IMAP хост, пробуем разобрать порт, если он есть
                         if ':' in new_value:
                             try:
                                 # Предполагаем формат host:port
                                 host_part, port_part = new_value.split(':', 1)
                                 # Проверяем, что портовая часть состоит только из цифр
                                 if port_part.isdigit():
                                     user_acc_data[user_id]['imap_host'] = host_part # Сохраняем только хост в imap_host
                                     user_acc_data[user_id]['imap_port'] = int(port_part) # Сохраняем порт в imap_port
                                     # Обновляем БД для imap_port тоже
                                     cursor.execute("UPDATE accounts SET imap_host = ?, imap_port = ? WHERE id = ?", (host_part, int(port_part), account_id))
                                     conn.commit()
                                     logger.info(f"[DB] Account {account_id}: updated imap_host to '{host_part}' and imap_port to {port_part}")
                                 else:
                                      # Если портовая часть не цифровая, сохраняем как есть в imap_host и ставим порт None
                                     user_acc_data[user_id]['imap_host'] = new_value # Сохраняем все в imap_host
                                     user_acc_data[user_id]['imap_port'] = None
                                     # Обновляем БД для imap_port тоже
                                     cursor.execute("UPDATE accounts SET imap_host = ?, imap_port = ? WHERE id = ?", (new_value, None, account_id))
                                     conn.commit()
                                     logger.warning(f"[DB] Account {account_id}: could not parse port from {new_value}. Saved as imap_host and imap_port set to None.")
                             except Exception as parse_e:
                                 logger.error(f"Error parsing imap_host '{new_value}': {parse_e}")
                                 # Если парсинг не удался, сохраняем как есть в imap_host и ставим порт None
                                 user_acc_data[user_id]['imap_host'] = new_value
                                 user_acc_data[user_id]['imap_port'] = None
                                 # Обновляем БД для imap_port тоже
                                 cursor.execute("UPDATE accounts SET imap_host = ?, imap_port = ? WHERE id = ?", (new_value, None, account_id))
                                 conn.commit()
                                 logger.warning(f"[DB] Account {account_id}: failed parsing imap_host '{new_value}'. Saved as imap_host and imap_port set to None.")
                         else:
                             # Если нет двоеточия, сохраняем как есть в imap_host и ставим порт None
                             user_acc_data[user_id]['imap_host'] = new_value
                             user_acc_data[user_id]['imap_port'] = None
                             # Обновляем БД для imap_port тоже
                             cursor.execute("UPDATE accounts SET imap_host = ?, imap_port = ? WHERE id = ?", (new_value, None, account_id))
                             conn.commit()
                             logger.info(f"[DB] Account {account_id}: updated imap_host to '{new_value}', no port found. imap_port set to None.")

                # Отправляем сообщение об успешном обновлении
                success_msg = bot.send_message(
                    message.chat.id,
                    f"✅ Данные успешно обновлены!\nАккаунт #{account_id}: изменен {field_to_change} с '{old_value}' на '{new_value}'"
                )

                # Удаляем сообщение об успехе через 3 секунды И затем возвращаемся в меню
                def delete_success_message():
                    try:
                        bot.delete_message(message.chat.id, success_msg.message_id)
                        logger.info(f"[INPUT] Сообщение об успехе удалено")
                    except Exception as e:
                        logger.error(f"[INPUT] Ошибка при удалении сообщения об успехе: {e}")
                    
                    # Возвращаемся в меню настроек входа ПОСЛЕ удаления сообщения об успехе
                    # Используем chat_id и request_message_id для возврата к сообщению с запросом ввода
                    # request_message_id это ID сообщения, которое мы отредактировали в запрос ввода,
                    # т.е. это исходное сообщение с меню настроек.
                    cb_login_settings(None, message.chat.id, state_info['request_message_id'], account_id)
                    logger.info(f"[INPUT] Возврат в меню настроек входа")


                threading.Timer(3.0, delete_success_message).start()

                # Важно: убираем прямой вызов cb_login_settings отсюда
                # cb_login_settings(None, message.chat.id, message.message_id, account_id)

            else:
                logger.warning(f"[DB] Attempted to update non-existent field: {field_to_change} for account {account_id}")
                bot.send_message(message.chat.id, f"❌ Не удалось обновить данные: поле '{field_to_change}' не найдено.")

        except Exception as e:
            logger.error(f"Error updating account data: {e}", exc_info=True)
            bot.send_message(message.chat.id, f"❌ Произошла ошибка при обновлении данных: {e}")

        finally:
            if conn:
                conn.close()
            # Очищаем состояние пользователя
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.callback_query_handler(func=lambda call: call.data.startswith('change_field:'))
    def cb_change_field(call):
        try:
            logger.info(f"[CALLBACK] Received callback: {call.data}")
            parts = call.data.split(':')
            account_id = parts[1]  # id теперь всегда строка
            field_to_change = parts[2]

            # Сохраняем состояние пользователя для ожидания ввода
            user_id = call.from_user.id
            user_states[user_id] = {
                'state': f'awaiting_input_{field_to_change}',
                'account_id': account_id,
                'request_message_id': call.message.message_id  # Сохраняем ID текущего сообщения
            }
            logger.info(f"[CALLBACK] Set user state: {user_states[user_id]}")

            # Запрашиваем у пользователя новые данные с кнопкой отмена
            keyboard = types.InlineKeyboardMarkup()
            cancel_button = types.InlineKeyboardButton("Отмена", callback_data=f"cancel_input:{account_id}")
            keyboard.add(cancel_button)

            prompt_message = f"✏️ Введите новое значение для поля '{field_to_change}':"
            sent_message = bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=prompt_message,
                reply_markup=keyboard,
                parse_mode="HTML"
            )

            bot.answer_callback_query(call.id, text=f"Введите новое значение для {field_to_change}")

        except Exception as e:
            logger.error(f"Error in cb_change_field: {e}")
            bot.answer_callback_query(call.id, text="Произошла ошибка.")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при запросе изменения данных: {e}")


    # Обработчик для кнопки "Отмена" при вводе данных
    @bot.callback_query_handler(func=lambda call: call.data.startswith('cancel_input:'))
    def cb_cancel_input(call):
        try:
            logger.info(f"[CALLBACK] Received callback: {call.data}")
            # --- Шаг 1: Отвечаем на callback query НЕМЕДЛЕННО ---
            bot.answer_callback_query(call.id, text="Ввод отменен.")

            account_id = call.data.split(':')[1]
            user_id = call.from_user.id
            state_info = user_states.get(user_id) # Получаем state_info для доступа к request_message_id

            # --- Шаг 2: Удаляем предыдущее сообщение бота с запросом (где была кнопка Отмена) ---
            # Используем message_id, сохраненный в user_states
            if state_info and 'request_message_id' in state_info:
                 try:
                      bot.delete_message(call.message.chat.id, state_info['request_message_id'])
                 except Exception as e:
                      logger.error(f"Error deleting request message on cancel: {e}")

            # --- Шаг 3: Очищаем состояние пользователя ---
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

            # --- Шаг 4: Возвращаемся в меню настроек входа ---
            # Вызываем cb_login_settings, передавая chat_id, message_id (ТЕКУЩЕГО сообщения call.message) и account_id
            # Мы передаем message_id ТЕКУЩЕГО сообщения, чтобы отредактировать ЕГО в меню настроек
            cb_login_settings(chat_id=call.message.chat.id, message_id=call.message.message_id, account_id=account_id)

        except Exception as e:
            logger.error(f"Error in cb_cancel_input for account {account_id}: {e}", exc_info=True)
            # Если answer_callback_query выше выбросил исключение, эта строка не выполнится.
            # В противном случае, отправляем сообщение об ошибке.
            bot.send_message(call.message.chat.id, f"❌ Произошла ошибка при отмене ввода: {e}")



    # --- СМЕНА ДАННЫХ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("chgdata:"))
    @auth_required
    def cb_change_data(call):
        bot.answer_callback_query(call.id)
        acc_id = call.data.split(":")[1]  # id теперь всегда строка
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT login, password, email_login, email_password, imap_host FROM accounts WHERE id=?", (acc_id,))
        row = c.fetchone()
        conn.close()
        if not row:
            bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
            return
        login, password, email_login, email_password, imap_host = row
        bot.send_message(call.message.chat.id, f"⏳ Запускаю процесс смены данных для <code>{login}</code>...", parse_mode="HTML")
    
        import threading
        def worker():
            import asyncio
            async def run_change():
                try:
                    logger.info(f"[AUTO_END_RENT] Начинаем процесс автоматической смены данных для аккаунта {acc_id}...")
                    from utils.browser_config import get_browser_config
                    screenshots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'screenshots')
                    os.makedirs(screenshots_dir, exist_ok=True)
                    logger.info(f"[AUTO_END_RENT] Папка для скриншотов создана: {screenshots_dir}")
                    logger.info(f"[AUTO_END_RENT] Запускаем Playwright...")
                    async with async_playwright() as p:
                        browser_config = get_browser_config()
                        logger.info(f"[AUTO_END_RENT] Запускаем браузер с конфигурацией: {browser_config}")
                        browser = await p.chromium.launch(**browser_config)
                        logger.info(f"[AUTO_END_RENT] Браузер успешно запущен")
                        
                        logger.info(f"[AUTO_END_RENT] Ждем 2 секунды...")
                        await asyncio.sleep(2)
                        
                        logger.info(f"[AUTO_END_RENT] Создаем новый контекст браузера...")
                        try:
                            # Создаем уникальную папку для данных браузера
                            user_data_dir = os.path.join(tempfile.gettempdir(), f"steam_browser_{acc_id}_{int(time.time())}")
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Создаю постоянный контекст: {user_data_dir}")
                            
                            # Создаем PERSISTENT контекст (НЕ инкогнито)
                            context = await browser.new_persistent_context(
                                user_data_dir=user_data_dir,
                                viewport={'width': 1920, 'height': 1080},
                                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                                ignore_https_errors=True,
                                java_script_enabled=True
                            )
                            
                            page = await context.new_page()
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Постоянный контекст создан")
                            
                        except Exception as e:
                            logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ошибка создания постоянного контекста: {e}")
                            # Fallback к обычному контексту
                            context = await browser.new_context()
                            page = await context.new_page()
                            logger.info(f"[AUTO_END_RENT] ✅ Контекст браузера создан успешно")
                            
                            logger.info(f"[AUTO_END_RENT] Создаем новую страницу...")
                            page = await context.new_page()
                            logger.info(f"[AUTO_END_RENT] ✅ Страница создана успешно")
                            
                            logger.info(f"[AUTO_END_RENT] Устанавливаем таймауты...")
                            page.set_default_timeout(30000)
                            page.set_default_navigation_timeout(30000)
                            logger.info(f"[AUTO_END_RENT] ✅ Таймауты установлены")
                            
                            logger.info(f"[AUTO_END_RENT] Переходим на страницу входа Steam...")
                            await page.goto('https://store.steampowered.com/login/')
                            logger.info(f"[AUTO_END_RENT] ✅ Переход на страницу выполнен")
                            
                            logger.info(f"[AUTO_END_RENT] Ждем полной загрузки страницы...")
                            await page.wait_for_load_state('networkidle')
                            logger.info(f"[AUTO_END_RENT] ✅ Страница полностью загружена")
                        except Exception as e:
                            logger.error(f"[AUTO_END_RENT] ❌ Ошибка при создании контекста/страницы: {e}")
                            raise
                        screenshot_path = os.path.join(screenshots_dir, f'login_page_{acc_id}.png')
                        try:
                            # Скриншот страницы входа
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Создаю скриншот страницы входа")
                            await page.screenshot(path=screenshot_path)
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Страница входа")
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Скриншот страницы входа создан и отправлен")
                        except Exception as e:
                            error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при создании скриншота страницы входа: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            bot.send_message(call.message.chat.id, error_msg)

                        try:
                            # Заполнение формы входа и обработка Steam Guard
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Начинаю заполнение формы входа")
                            await page.wait_for_selector('input[type="text"]', timeout=20000)
                            await page.fill('input[type="text"]', login)
                            await page.fill('input[type="password"]', password)
                            await page.click("button[type='submit']")
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Форма входа заполнена, ожидаю ответ")
                            
                            await page.wait_for_selector("#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=25000)
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Получен ответ от Steam")
                            
                            # Проверка необходимости Steam Guard
                            need_guard = False
                            if await page.query_selector("#auth_buttonset_entercode"):
                                need_guard = True
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Обнаружена форма Steam Guard (entercode)")
                            elif await page.query_selector("input[maxlength='1']"):
                                need_guard = True
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Обнаружена форма Steam Guard (digit inputs)")
                                
                            if need_guard:
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Требуется код Steam Guard")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Необходимо ввести код Steam Guard. Получаю код с почты...")
                                
                                try:
                                    # Получение кода Steam Guard для входа
                                    from utils.email_utils import fetch_steam_guard_code_from_email
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Получаю код Steam Guard с почты")
                                    code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, logger=logger, mode='login')
                                    
                                    if code:
                                        logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Код Steam Guard получен: {code}")
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Получен код: {code}")
                                        
                                        try:
                                            # Ввод кода Steam Guard
                                            if await page.query_selector("input[maxlength='1']"):
                                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Заполняю цифровые поля кода")
                                                inputs = await page.query_selector_all("input[maxlength='1']")
                                                if len(inputs) == len(code):
                                                    for i, ch in enumerate(code):
                                                        await inputs[i].fill(ch)
                                                    await asyncio.sleep(2)
                                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Код введен в цифровые поля")
                                                else:
                                                    logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Несоответствие количества полей ({len(inputs)}) и символов кода ({len(code)})")
                                            elif await page.query_selector("input[name='authcode']"):
                                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Заполняю текстовое поле кода")
                                                await page.fill("input[name='authcode']", code)
                                                await asyncio.sleep(1)
                                                submit_btn = await page.query_selector("button[type='submit']")
                                                if submit_btn:
                                                    await submit_btn.click()
                                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Кнопка отправки кода нажата")
                                            await asyncio.sleep(5)
                                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Код Steam Guard обработан")
                                            
                                        except Exception as e:
                                            error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при вводе кода Steam Guard: {str(e)}"
                                            logger.error(error_msg, exc_info=True)
                                            bot.send_message(call.message.chat.id, error_msg)
                                            return
                                            
                                    else:
                                        error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Не удалось получить код Steam Guard"
                                        logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] {error_msg}")
                                        bot.send_message(call.message.chat.id, error_msg)
                                        return
                                        
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при получении кода Steam Guard: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return
                            else:
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Steam Guard не требуется")
                                
                        except Exception as e:
                            error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при заполнении формы входа: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            bot.send_message(call.message.chat.id, error_msg)
                            return
                        try:
                            # Проверка успешного входа с детальной диагностикой
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Начинаю детальную диагностику входа...")
                            
                            # 1. Проверяем текущий URL
                            current_url = page.url
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Текущий URL: {current_url}")
                            
                            # 2. Проверяем заголовок страницы
                            page_title = await page.title()
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Заголовок страницы: {page_title}")
                            
                            # 3. Делаем диагностический скриншот
                            diagnostic_screenshot_path = os.path.join(screenshots_dir, f'diagnostic_{acc_id}.png')
                            await page.screenshot(path=diagnostic_screenshot_path, full_page=True)
                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Диагностический скриншот создан")
                            
                            # 4. Проверяем наличие различных элементов на странице
                            elements_to_check = {
                                'account_pulldown': '#account_pulldown',
                                'login_form': '.newlogindialog',
                                'error_message': '.newlogindialog_FormError',
                                'captcha': '#captchagid',
                                'guard_code_input': 'input[maxlength="1"]',
                                'guard_text_input': 'input[name="authcode"]',
                                'username_field': 'input[type="text"]',
                                'password_field': 'input[type="password"]',
                                'submit_button': 'button[type="submit"]',
                                'loading_indicator': '.loading',
                                'ban_message': '.auth_modal_h1',
                                'rate_limit': '.newlogindialog_FormError:has-text("rate")',
                                'maintenance': 'text=maintenance',
                                'suspended': 'text=suspended',
                                'player_avatar': '.playerAvatar',
                                'store_nav': '.store_nav_area',
                                # ✅ ДОБАВИТЬ ЭТИ СТРОКИ:
                                'steam_error': 'text="При входе в аккаунт произошла ошибка"',
                                'retry_button': 'text="Повторить"',
                                'general_error': 'text="Ошибка"'
                            }
                            
                            found_elements = {}
                            for element_name, selector in elements_to_check.items():
                                try:
                                    element = await page.query_selector(selector)
                                    if element:
                                        try:
                                            element_text = await element.inner_text()
                                            found_elements[element_name] = {
                                                'found': True,
                                                'text': element_text[:100],  # Первые 100 символов
                                                'visible': await element.is_visible()
                                            }
                                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Найден {element_name}: '{element_text[:50]}'")
                                        except:
                                            found_elements[element_name] = {'found': True, 'text': '[текст недоступен]', 'visible': True}
                                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Найден {element_name}")
                                    else:
                                        found_elements[element_name] = {'found': False}
                                        logger.debug(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Не найден {element_name}")
                                except Exception as e:
                                    found_elements[element_name] = {'found': False, 'error': str(e)}
                                    logger.debug(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Ошибка поиска {element_name}: {e}")
                            
                            # 5. Анализируем что мы нашли
                            if (found_elements.get('steam_error', {}).get('found') or 
                                found_elements.get('retry_button', {}).get('found') or
                                found_elements.get('general_error', {}).get('found')):
                                
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ⚠️ Обнаружена ошибка Steam - пробуем кнопку Повторить")
                                
                                # Ищем и нажимаем кнопку "Повторить"
                                retry_selectors = [
                                    'text="Повторить"',
                                    'text="Retry"',
                                    'button:has-text("Повторить")',
                                    'button:has-text("Retry")',
                                    'a:has-text("Повторить")'
                                ]
                                
                                retry_clicked = False
                                for selector in retry_selectors:
                                    try:
                                        retry_btn = await page.query_selector(selector)
                                        if retry_btn and await retry_btn.is_visible():
                                            await retry_btn.click()
                                            logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Кнопка Повторить нажата: {selector}")
                                            retry_clicked = True
                                            break
                                    except Exception as e:
                                        logger.debug(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Селектор {selector} не сработал: {e}")
                                        continue
                                
                                if retry_clicked:
                                    # Ждем после нажатия кнопки (больше времени для persistent context)
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ждем 20 секунд после нажатия Повторить...")
                                    bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ⏳ Повторяем попытку...")
                                    await asyncio.sleep(20)  # Увеличено время ожидания
                                    
                                    # НЕ очищаем куки - пусть persistent context сохраняет состояние
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Persistent context сохраняет состояние сессии")
                                    
                                    # Проверяем результат (может потребоваться обновление страницы)
                                    try:
                                        await page.reload(wait_until='networkidle')
                                        logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Страница обновлена после повтора")
                                    except Exception as e:
                                        logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ошибка обновления страницы: {e}")
                                    
                                    # Проверяем результат повтора
                                    if await page.query_selector("#account_pulldown"):
                                        logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Повтор успешен! Вход выполнен!")
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ✅ Повтор успешен!")
                                        # ПЕРЕХОДИМ К КОДУ СМЕНЫ ПАРОЛЯ (см. ниже)
                                    else:
                                        logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Повтор не помог")
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Повтор не помог, попробуйте позже")
                                        
                                        # Диагностический скриншот
                                        with open(diagnostic_screenshot_path, 'rb') as photo:
                                            bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Повтор не помог")
                                        return
                                else:
                                    logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Кнопка Повторить не найдена")
                                    bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Ошибка Steam, кнопка Повторить не найдена")
                                    
                                    with open(diagnostic_screenshot_path, 'rb') as photo:
                                        bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка Steam")
                                    return

                            elif found_elements.get('account_pulldown', {}).get('found'):                            
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Успешный вход подтвержден!")
                                
                                # ✅ ВСТАВИТЬ СЮДА ВЕСЬ КОД СМЕНЫ ПАРОЛЯ:
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ✅ Успешный вход!")
                                
                                try:
                                    # Скриншот успешного входа
                                    screenshot_path = os.path.join(screenshots_dir, f'login_success_{acc_id}.png')
                                    await page.screenshot(path=screenshot_path)
                                    with open(screenshot_path, 'rb') as photo:
                                        bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Успешный вход")
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Скриншот успешного входа отправлен")
                                except Exception as e:
                                    logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ошибка при создании скриншота успешного входа: {str(e)}", exc_info=True)

                                try:
                                    # Переход на страницу смены пароля
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Перехожу на страницу смены пароля")
                                    await page.goto('https://help.steampowered.com/wizard/HelpChangePassword?redir=store/account/')
                                    await page.wait_for_load_state('networkidle')
                                    await asyncio.sleep(2)
                                    
                                    screenshot_path = os.path.join(screenshots_dir, f'password_change_page_{acc_id}.png')
                                    await page.screenshot(path=screenshot_path)
                                    with open(screenshot_path, 'rb') as photo:
                                        bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Страница смены пароля")
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Переход на страницу смены пароля выполнен")
                                    
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при переходе на страницу смены пароля: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Отправка кода на почту
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Нажимаю кнопку отправки кода")
                                    await page.click('a.help_wizard_button')
                                    await asyncio.sleep(2)
                                    
                                    screenshot_path = os.path.join(screenshots_dir, f'code_sent_{acc_id}.png')
                                    await page.screenshot(path=screenshot_path)
                                    with open(screenshot_path, 'rb') as photo:
                                        bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Код отправлен на почту")
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Код отправлен на почту")
                                    
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при отправке кода: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Получение кода с почты для смены пароля
                                    logger.info(f"[AUTO_END_RENT] Начинаем получение кода с почты...")
                                    logger.info(f"[AUTO_END_RENT] Email: {email_login[:3]}***@{email_login.split('@')[1] if '@' in email_login else 'unknown'}")
                                    logger.info(f"[AUTO_END_RENT] IMAP хост: {imap_host}")
                                    
                                    from utils.email_utils import fetch_steam_guard_code_from_email
                                    
                                    logger.info(f"[AUTO_END_RENT] Вызываем функцию получения кода с таймаутом 180 секунд...")
                                    code = fetch_steam_guard_code_from_email(
                                        email_login, 
                                        email_password, 
                                        imap_host, 
                                        logger=logger, 
                                        mode='change',
                                        timeout=180
                                    )
                                    logger.info(f"[AUTO_END_RENT] Функция получения кода завершена. Результат: {'✅ Код получен' if code else '❌ Код не получен'}")
                                    
                                    if not code:
                                        error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Не удалось получить код с почты"
                                        logger.error(f"[AUTO_END_RENT] {error_msg}")
                                        bot.send_message(call.message.chat.id, error_msg)
                                        return
                                    else:
                                        logger.info(f"[AUTO_END_RENT] Код получен с почты: {code[:3]}***")
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ✅ Код получен: {code[:3]}***")
                                        
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при получении кода с почты: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Ввод кода для смены пароля
                                    logger.info(f"[AUTO_END_RENT] Заполняем поле с кодом...")
                                    
                                    code_selectors = [
                                        'input[name="authcode"]',
                                        'input[id="authcode"]', 
                                        'input[placeholder*="code"]',
                                        'input[placeholder*="код"]',
                                        'input[type="text"]',
                                        'input.authcode_entry_input',
                                        '#authcode_entry',
                                        '.authcode_entry input'
                                    ]
                                    
                                    code_filled = False
                                    for selector in code_selectors:
                                        try:
                                            logger.info(f"[AUTO_END_RENT] Пробуем селектор для кода: {selector}")
                                            
                                            await page.wait_for_selector(selector, timeout=3000)
                                            logger.info(f"[AUTO_END_RENT] ✅ Элемент найден: {selector}")
                                            
                                            await page.fill(selector, "", timeout=3000)
                                            logger.info(f"[AUTO_END_RENT] Поле очищено")
                                            
                                            await page.fill(selector, code, timeout=5000)
                                            logger.info(f"[AUTO_END_RENT] ✅ Код введен: {code}")
                                            
                                            entered_value = await page.input_value(selector)
                                            logger.info(f"[AUTO_END_RENT] Проверка введенного значения: {entered_value}")
                                            
                                            if entered_value == code:
                                                logger.info(f"[AUTO_END_RENT] ✅ Код успешно введен и проверен через селектор: {selector}")
                                                code_filled = True
                                                break
                                            else:
                                                logger.warning(f"[AUTO_END_RENT] Код введен некорректно: ожидали {code}, получили {entered_value}")
                                                
                                        except Exception as e:
                                            logger.warning(f"[AUTO_END_RENT] Селектор {selector} не сработал: {e}")
                                            continue
                                    
                                    if not code_filled:
                                        error_msg = f"[AUTO_END_RENT] ❌ Не удалось найти поле для ввода кода"
                                        logger.error(error_msg)
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Поле для ввода кода не найдено.")
                                        return
                                        
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при вводе кода: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Поиск и нажатие кнопки Continue
                                    logger.info(f"[AUTO_END_RENT] Нажимаем кнопку Continue...")
                                    
                                    continue_selectors = [
                                        'button:has-text("Continue")',
                                        'input[type="submit"]',
                                        'button[type="submit"]',
                                        'button:has-text("Продолжить")',
                                        '.auth_button_set .auth_button',
                                        '#auth_continue_button'
                                    ]
                                    
                                    continue_clicked = False
                                    for selector in continue_selectors:
                                        try:
                                            logger.info(f"[AUTO_END_RENT] Пробуем найти кнопку Continue: {selector}")
                                            
                                            await page.wait_for_selector(selector, timeout=3000)
                                            logger.info(f"[AUTO_END_RENT] ✅ Кнопка найдена: {selector}")
                                            
                                            # Скриншот перед нажатием
                                            try:
                                                screenshot_path = os.path.join(screenshots_dir, f'before_continue_click_{acc_id}.png')
                                                await page.screenshot(path=screenshot_path)
                                                logger.info(f"[AUTO_END_RENT] Скриншот перед нажатием кнопки сохранен")
                                            except Exception as screenshot_e:
                                                logger.warning(f"[AUTO_END_RENT] Ошибка при создании скриншота перед нажатием: {screenshot_e}")
                                            
                                            await page.click(selector, timeout=3000)
                                            logger.info(f"[AUTO_END_RENT] ✅ Кнопка Continue нажата через селектор: {selector}")
                                            
                                            await asyncio.sleep(2)
                                            
                                            # Скриншот после нажатия
                                            try:
                                                screenshot_path = os.path.join(screenshots_dir, f'after_continue_click_{acc_id}.png')
                                                await page.screenshot(path=screenshot_path)
                                                logger.info(f"[AUTO_END_RENT] Скриншот после нажатия кнопки сохранен")
                                            except Exception as screenshot_e:
                                                logger.warning(f"[AUTO_END_RENT] Ошибка при создании скриншота после нажатия: {screenshot_e}")
                                            
                                            current_url = page.url
                                            logger.info(f"[AUTO_END_RENT] URL после клика: {current_url}")
                                            
                                            continue_clicked = True
                                            break
                                            
                                        except Exception as e:
                                            logger.warning(f"[AUTO_END_RENT] Селектор кнопки {selector} не сработал: {e}")
                                            continue
                                    
                                    if not continue_clicked:
                                        error_msg = f"[AUTO_END_RENT] ❌ Не удалось найти кнопку Continue"
                                        logger.error(error_msg)
                                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Кнопка подтверждения не найдена.")
                                        return
                                    
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при нажатии кнопки Continue: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Ожидание после нажатия кнопки
                                    logger.info(f"[AUTO_END_RENT] Ждем 5 секунд после клика...")
                                    await asyncio.sleep(5)
                                    
                                    # Проверяем, что страница еще активна
                                    if page.is_closed():
                                        error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Браузер закрылся после ввода кода"
                                        logger.error(f"[AUTO_END_RENT] {error_msg}")
                                        bot.send_message(call.message.chat.id, error_msg)
                                        return
                                    else:
                                        logger.info(f"[AUTO_END_RENT] ✅ Страница активна")
                                        current_url = page.url
                                        logger.info(f"[AUTO_END_RENT] Текущий URL: {current_url}")
                                        
                                        # Проверяем, не появилась ли ошибка на странице
                                        try:
                                            page_content = await page.content()
                                            if "error" in page_content.lower() or "ошибка" in page_content.lower():
                                                logger.warning(f"[AUTO_END_RENT] Обнаружена ошибка на странице")
                                                screenshot_path = os.path.join(screenshots_dir, f'error_after_code_{acc_id}.png')
                                                await page.screenshot(path=screenshot_path)
                                                with open(screenshot_path, 'rb') as photo:
                                                    bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка после ввода кода")
                                        except Exception as e:
                                            logger.warning(f"[AUTO_END_RENT] Ошибка при проверке содержимого страницы: {e}")
                                        
                                        logger.info(f"[AUTO_END_RENT] ✅ Код успешно обработан")
                                        
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка при проверке состояния страницы: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)
                                    return

                                try:
                                    # Финальные проверки и завершение
                                    logger.info(f"[AUTO_END_RENT] Проверяем финальное состояние...")
                                    current_url = page.url
                                    logger.info(f"[AUTO_END_RENT] Финальный URL: {current_url}")
                                    
                                    # Ждем возможного редиректа
                                    try:
                                        await page.wait_for_load_state('networkidle', timeout=10000)
                                        logger.info(f"[AUTO_END_RENT] Страница загружена")
                                    except Exception as e:
                                        logger.warning(f"[AUTO_END_RENT] Timeout при ожидании загрузки: {e}")
                                    
                                    # Финальный скриншот
                                    try:
                                        screenshot_path = os.path.join(screenshots_dir, f'final_result_{acc_id}.png')
                                        await page.screenshot(path=screenshot_path)
                                        with open(screenshot_path, 'rb') as photo:
                                            bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ✅ Процесс завершен")
                                        logger.info(f"[AUTO_END_RENT] Финальный скриншот отправлен")
                                    except Exception as e:
                                        logger.warning(f"[AUTO_END_RENT] Ошибка при создании финального скриншота: {e}")
                                    
                                    # Успешное завершение
                                    logger.info(f"[AUTO_END_RENT] ✅ Процесс смены пароля завершен успешно!")
                                    bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ✅ Смена пароля завершена!")
                                    
                                except Exception as e:
                                    error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка финальных проверок: {str(e)}"
                                    logger.error(error_msg, exc_info=True)
                                    bot.send_message(call.message.chat.id, error_msg)

                                # ЗАВЕРШЕНИЕ УСПЕШНОГО БЛОКА
                                return

                                
                            elif found_elements.get('error_message', {}).get('found'):
                                error_text = found_elements['error_message'].get('text', 'Неизвестная ошибка')
                                logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Ошибка Steam: {error_text}")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Ошибка Steam: {error_text}")
                                
                                # Отправляем скриншот с ошибкой
                                with open(diagnostic_screenshot_path, 'rb') as photo:
                                    bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка входа")
                                return
                                
                            elif found_elements.get('captcha', {}).get('found'):
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ⚠️ Требуется капча")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ⚠️ Требуется капча")
                                
                                with open(diagnostic_screenshot_path, 'rb') as photo:
                                    bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Требуется капча")
                                return
                                
                            elif found_elements.get('rate_limit', {}).get('found'):
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ⚠️ Превышен лимит попыток входа")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ⚠️ Превышен лимит попыток входа")
                                return
                                
                            elif found_elements.get('suspended', {}).get('found') or found_elements.get('ban_message', {}).get('found'):
                                ban_text = found_elements.get('ban_message', {}).get('text', 'Аккаунт заблокирован')
                                logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Аккаунт заблокирован: {ban_text}")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Аккаунт заблокирован")
                                return
                                
                            elif found_elements.get('maintenance', {}).get('found'):
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ⚠️ Steam на техническом обслуживании")
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ⚠️ Steam на техническом обслуживании")
                                return
                                
                            elif found_elements.get('login_form', {}).get('found'):
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ⚠️ Все еще на странице входа - возможно неверные данные")
                                
                                # Проверяем, заполнены ли поля
                                if found_elements.get('username_field', {}).get('found'):
                                    try:
                                        username_value = await page.input_value('input[type="text"]')
                                        logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Значение поля логина: '{username_value}'")
                                    except:
                                        pass
                                        
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ⚠️ Остались на странице входа")
                                
                            else:
                                logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Неизвестное состояние страницы")
                                
                                # Логируем все найденные элементы
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Найденные элементы:")
                                for name, data in found_elements.items():
                                    if data.get('found'):
                                        logger.info(f"  - {name}: {data.get('text', 'N/A')[:30]}")
                            
                            # 6. Проверяем cookies
                            try:
                                cookies = await page.context.cookies()
                                steam_cookies = [c for c in cookies if 'steam' in c.get('name', '').lower()]
                                logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Найдено {len(steam_cookies)} Steam cookies")
                                
                                login_cookies = [c for c in cookies if 'steamLoginSecure' in c.get('name', '')]
                                if login_cookies:
                                    logger.info(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ✅ Найдены cookies авторизации")
                                else:
                                    logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Cookies авторизации не найдены")
                                    
                            except Exception as cookie_e:
                                logger.warning(f"[STEAM][ID: {acc_id}][LOGIN: {login}] Ошибка проверки cookies: {cookie_e}")
                            
                            # 7. Отправляем диагностический отчет
                            diagnostic_report = f"""
                        🔍 <b>Диагностический отчет</b>
                        <b>ID:</b> {acc_id}
                        <b>Логин:</b> {html.escape(login)}
                        <b>URL:</b> {current_url}
                        <b>Заголовок:</b> {page_title}

                        <b>Найденные элементы:</b>
                        """
                            
                            for name, data in found_elements.items():
                                if data.get('found'):
                                    status = "✅"
                                    text = data.get('text', '')[:30]
                                    diagnostic_report += f"{status} {name}: {text}\n"
                            
                            bot.send_message(call.message.chat.id, diagnostic_report, parse_mode="HTML")
                            
                            # Отправляем скриншот
                            with open(diagnostic_screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Диагностический скриншот")
                            
                            # Если #account_pulldown не найден, завершаем с ошибкой
                            if not found_elements.get('account_pulldown', {}).get('found'):
                                logger.error(f"[STEAM][ID: {acc_id}][LOGIN: {login}] ❌ Вход не удался - элемент #account_pulldown не найден")
                                return
                                
                        except Exception as e:
                            error_msg = f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка диагностики входа: {str(e)}"
                            logger.error(error_msg, exc_info=True)
                            bot.send_message(call.message.chat.id, error_msg)
                                            

                            # Генерируем новый пароль
                            logger.info(f"[AUTO_END_RENT] Генерируем новый пароль...")
                            from utils.password import generate_password
                            new_password = generate_password()
                            logger.info(f"[AUTO_END_RENT] Новый пароль сгенерирован: {new_password[:3]}***")
                            
                            # Получаем старый пароль из БД перед обновлением
                            logger.info(f"[AUTO_END_RENT] Получаем старый пароль из БД для аккаунта {acc_id}...")
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute('SELECT password FROM accounts WHERE id = ?', (acc_id,))
                            result = c.fetchone()
                            old_password_db = result[0] if result else "Неизвестно"
                            logger.info(f"[AUTO_END_RENT] Старый пароль из БД: {old_password_db[:3]}***")
                            conn.close()

                            logger.info(f"[AUTO_END_RENT] Ищем поля для ввода нового пароля...")
                            password_fields = await page.query_selector_all('input[type="password"]')
                            logger.info(f"[AUTO_END_RENT] Найдено {len(password_fields)} полей типа password")
                            
                            if len(password_fields) >= 2:
                                logger.info(f"[AUTO_END_RENT] Заполняем первое поле пароля...")
                                await password_fields[0].fill(new_password)
                                logger.info(f"[AUTO_END_RENT] Заполняем второе поле пароля...")
                                await password_fields[1].fill(new_password)
                                logger.info(f"[AUTO_END_RENT] Оба поля пароля заполнены")
                            else:
                                logger.info(f"[AUTO_END_RENT] Недостаточно полей password, пробуем альтернативные селекторы...")
                                filled = False
                                for sel in [
                                    'input[placeholder*="Change my password"]',
                                    'input[placeholder*="Re-enter"]',
                                    'input[name*="new_password"]',
                                    'input[name*="reenter"]'
                                ]:
                                    try:
                                        logger.info(f"[AUTO_END_RENT] Пробуем селектор: {sel}")
                                        await page.fill(sel, new_password)
                                        logger.info(f"[AUTO_END_RENT] Успешно заполнен селектор: {sel}")
                                        filled = True
                                        break
                                    except Exception as e:
                                        logger.warning(f"[AUTO_END_RENT] Не удалось заполнить {sel}: {e}")
                                        continue
                                
                                if not filled:
                                    logger.error(f"[AUTO_END_RENT] Не удалось заполнить ни одно поле пароля!")
                            screenshot_path = os.path.join(screenshots_dir, f'password_ready_{acc_id}.png')
                            await page.screenshot(path=screenshot_path)
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Пароль готов к смене")
                            
                            logger.info(f"[AUTO_END_RENT] Переходим к нажатию кнопки смены пароля...")
                            
                            click_successful = False
                            
                            # После заполнения полей пароля
                            logger.info(f"[AUTO_END_RENT] Ждем 1 секунду перед кликом...")
                            await asyncio.sleep(1)
                            
                            # Первая попытка клика
                            logger.info(f"[AUTO_END_RENT] Ищем кнопку смены пароля...")
                            for sel in [
                                'button:has-text("Сменить пароль"):not([disabled])',
                                'button:has-text("Сменить пароль")',
                                '#change_password_button',
                                '.change_password_button',
                                'button:has-text("Change Password"):not([disabled])',
                                'button:has-text("Change Password")',
                                'button[type="submit"]',
                                'input[type="submit"]',
                            ]:
                                try:
                                    logger.info(f"[AUTO_END_RENT] Пробуем кликнуть селектор: {sel}")
                                    await page.click(sel, timeout=3000)
                                    logger.info(f"[AUTO_END_RENT] ✅ Успешно нажали на кнопку смены пароля: {sel}")
                                    clicked = True
                                    break
                                except Exception as e:
                                    logger.warning(f"[AUTO_END_RENT] Не удалось кликнуть {sel}: {e}")
                                    continue
                                    
                            if not clicked:
                                logger.error("[AUTO_END_RENT] ❌ Не удалось нажать на кнопку смены пароля ни одним из селекторов!")
                                screenshot_fail_path = os.path.join(screenshots_dir, f"auto_end_button_fail_{acc_id}.png")
                                await page.screenshot(path=screenshot_fail_path)
                                bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Не найдена кнопка смены пароля")
                                with open(screenshot_fail_path, 'rb') as photo:
                                    bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}] Не найдена кнопка смены")
                                return
                            
                            logger.info(f"[AUTO_END_RENT] Ждем завершения операции смены пароля...")
                            await page.wait_for_load_state('networkidle')
                            
                            logger.info(f"[AUTO_END_RENT] Делаем финальный скриншот...")
                            screenshot_path = os.path.join(screenshots_dir, f'password_changed_{acc_id}.png')
                            await page.screenshot(path=screenshot_path)
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Пароль изменен")
                            
                            logger.info(f"[AUTO_END_RENT] Обновляем пароль в базе данных...")
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute('UPDATE accounts SET password = ? WHERE id = ?', (new_password, acc_id))
                            conn.commit()
                            conn.close()
                            logger.info(f"[AUTO_END_RENT] ✅ Пароль успешно обновлен в БД для аккаунта {acc_id}")
                            # Отправляем сообщение администраторам с новым и старым паролем
                            message_to_admin = (
                                f"🔑 Пароль изменён\n"
                                f"ID: {acc_id}\n"
                                f"Логин: {html.escape(login)}\n"
                                f"Старый пароль: <code>{html.escape(old_password_db)}</code>\n"
                                f"Новый пароль: <code>{html.escape(new_password)}</code>"
                            )
                            for admin_id in ADMIN_IDS:
                                try:
                                    bot.send_message(admin_id, message_to_admin, parse_mode="HTML")
                                except Exception as admin_msg_e:
                                    print(f"Не удалось отправить сообщение админу {admin_id}: {admin_msg_e}")
                            # Удаляем отправку сообщения пользователю

                        except Exception as e:
                            logger.error(f"[AUTO_END_RENT] ❌ Критическая ошибка в процессе смены пароля: {e}")
                            logger.error(f"[AUTO_END_RENT] Тип ошибки: {type(e).__name__}")
                            import traceback
                            logger.error(f"[AUTO_END_RENT] Трассировка стека: {traceback.format_exc()}")
                            
                            screenshot_path = os.path.join(screenshots_dir, f'error_{acc_id}.png')
                            await page.screenshot(path=screenshot_path)
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, caption=f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] Ошибка: {html.escape(str(e))}")
                            html_content = await page.content()
                            html_path = os.path.join(screenshots_dir, f'error_page_{acc_id}.html')
                            with open(html_path, 'w', encoding='utf-8') as f:
                                f.write(html_content)
                            raise Exception(f"Ошибка: {html.escape(str(e))}")

                except Exception as e:
                    logger.error(f"[AUTO_END_RENT] ❌ Общая ошибка в worker: {e}")
                    import traceback
                    logger.error(f"[AUTO_END_RENT] Полная трассировка: {traceback.format_exc()}")
                    
                    # Определяем тип ошибки и отправляем соответствующее сообщение
                    error_str = str(e)
                    if "Timeout" in error_str or "timeout" in error_str:
                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Достигнуто максимальное количество попыток смены пароля. Таймаут при взаимодействии со страницей Steam.")
                    elif "fill" in error_str and "input" in error_str:
                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Достигнуто максимальное количество попыток смены пароля. Поле ввода не найдено.")
                    elif "click" in error_str:
                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Достигнуто максимальное количество попыток смены пароля. Кнопка не найдена.")
                    else:
                        bot.send_message(call.message.chat.id, f"[STEAM][ID: {acc_id}][LOGIN: {html.escape(login)}] ❌ Достигнуто максимальное количество попыток смены пароля. Техническая ошибка.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run_change())
        threading.Thread(target=worker, daemon=True).start()

    # --- ОБРАБОТЧИК СООБЩЕНИЙ ДЛЯ КАСТОМНОГО ВРЕМЕНИ АРЕНДЫ ---
    @bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('state') == 'awaiting_custom_rent_time')
    def handle_custom_rent_time(message):
        user_id = message.from_user.id
        state_info = user_states.get(user_id, {})
        
        if not state_info:
            return
            
        try:
            # Пытаемся преобразовать введенное время в число
            hours = float(message.text.replace(',', '.'))
            
            # Проверяем разумность времени (от 0.1 часа до 720 часов = 30 дней)
            if hours < 0.1:
                bot.send_message(message.chat.id, "❌ Минимальное время аренды - 0.1 часа (6 минут)")
                return
                
            if hours > 720:
                bot.send_message(message.chat.id, "❌ Максимальное время аренды - 720 часов (30 дней)")
                return
            
            # Удаляем сообщение пользователя с временем
            try:
                bot.delete_message(message.chat.id, message.message_id)
            except Exception:
                pass
                
            # Очищаем состояние пользователя
            user_states.pop(user_id, None)
            
            # Выполняем аренду напрямую без создания mock call
            # Получаем информацию об аккаунте
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT * FROM accounts WHERE id=?", (state_info['account_id'],))
            acc = c.fetchone()
            conn.close()
            
            if not acc:
                bot.send_message(message.chat.id, "❌ Аккаунт не найден")
                return
                
            # Проверяем, не арендован ли уже аккаунт
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT rented_until FROM accounts WHERE id=? AND rented_until > ?", 
                     (state_info['account_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            if c.fetchone():
                conn.close()
                bot.send_message(message.chat.id, f"❌ Аккаунт #{acc[0]} уже арендован")
                return
            conn.close()
            
            # Выполняем аренду
            try:
                rent_seconds = int(hours * 3600)
                rented_until = datetime.now() + timedelta(seconds=rent_seconds)
                rented_until_str = rented_until.strftime('%d.%m.%Y, %H:%M (MSK)')
                
                # Обновляем БД
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE accounts SET rented_until=?, rented_by=?, order_id=? WHERE id=?", 
                         (rented_until.strftime('%Y-%m-%d %H:%M:%S'), user_id, None, state_info['account_id']))
                conn.commit()
                conn.close()
                
                logger.debug(f"[RENT] Аккаунт {state_info['account_id']} помечен как арендованный до {rented_until_str} для пользователя {user_id} с order_id None")
                
                # Запускаем таймер завершения аренды
                from steam.steam_account_rental_utils import auto_end_rent
                auto_end_rent(state_info['account_id'], user_id, rent_seconds)
                
                # Форматируем время аренды для сообщения
                if hours == int(hours):
                    hours_text = f"{int(hours)} часов"
                else:
                    hours_text = f"{hours} часов"
                    
                # Отправляем сообщение об успешной аренде
                rent_msg = (
                    f"✅ Аккаунт #{acc[0]} ({acc[1]}) арендован на {hours_text}!\n"
                    f"Аренда закончится: {rented_until_str}\n"
                    f"Логин: <code>{acc[1]}</code>\n"
                    f"Пароль: <code>{acc[2]}</code>"
                )
                bot.send_message(message.chat.id, rent_msg, parse_mode="HTML")
                
            except Exception as e:
                logger.error(f"Ошибка при выполнении аренды: {e}")
                bot.send_message(message.chat.id, "❌ Ошибка при выполнении аrenды")
            
        except ValueError:
            bot.send_message(message.chat.id, "❌ Неверный формат времени. Введите число (например: 5 или 0.5 для 30 минут)")
            
        except Exception as e:
            logger.error(f"Ошибка при обработке кастомного времени аренды: {e}")
            bot.send_message(message.chat.id, "❌ Произошла ошибка при обработке времени аренды")
            user_states.pop(user_id, None)

    # --- НОВЫЕ ОБРАБОТЧИКИ ДЛЯ УЛУЧШЕННОГО UI ---
    
    @bot.callback_query_handler(func=lambda c: c.data == "main_menu")
    @auth_required
    def cb_main_menu(call):
        """Возврат в главное меню"""
        bot.answer_callback_query(call.id)
        welcome_text = (
            "🚀 <b>Steam Rental Bot v2.0</b>\n\n"
            "💡 <b>Добро пожаловать в систему управления арендой Steam аккаунтов!</b>\n\n"
            "📋 <b>Управление аккаунтами</b> - просмотр, добавление и настройка аккаунтов\n"
            "➕ <b>Добавить новый аккаунт</b> - регистрация нового Steam аккаунта\n"
            "📊 <b>Статистика и аналитика</b> - отчеты по аренде и доходам\n"
            "⚙️ <b>Настройки системы</b> - конфигурация уведомлений и безопасности\n"
            "💬 <b>Техподдержка</b> - связь с администратором\n\n"
            "🔥 Выберите действие для начала работы:"
        )
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               welcome_text, main_menu(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "stats")
    @auth_required
    def cb_stats_menu(call):
        """Показать меню статистики"""
        bot.answer_callback_query(call.id)
        
        # Получаем основную статистику из базы данных
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Общее количество аккаунтов
            c.execute("SELECT COUNT(*) FROM accounts")
            total_accounts = c.fetchone()[0]
            
            # Количество свободных аккаунтов
            c.execute("SELECT COUNT(*) FROM accounts WHERE status = 'free'")
            free_accounts = c.fetchone()[0]
            
            # Количество арендованных аккаунтов
            rented_accounts = total_accounts - free_accounts
            
            # Уникальные игры
            c.execute("SELECT COUNT(DISTINCT game) FROM accounts")
            unique_games = c.fetchone()[0]
            
            conn.close()
            
            stats_text = (
                f"📊 <b>Статистика системы</b>\n\n"
                f"🎮 <b>Всего аккаунтов:</b> {total_accounts}\n"
                f"🟢 <b>Свободные:</b> {free_accounts}\n"
                f"🔴 <b>В аренде:</b> {rented_accounts}\n"
                f"🎯 <b>Уникальных игр:</b> {unique_games}\n\n"
                f"📈 Выберите тип отчета для детального анализа:"
            )
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            stats_text = (
                "📊 <b>Статистика системы</b>\n\n"
                "❌ Ошибка при загрузке данных\n\n"
                "📈 Выберите тип отчета:"
            )
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               stats_text, stats_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "settings")
    @auth_required  
    def cb_settings_menu(call):
        """Показать меню настроек"""
        bot.answer_callback_query(call.id)
        
        settings_text = (
            "⚙️ <b>Настройки системы</b>\n\n"
            "🔧 <b>Конфигурация Steam Rental Bot</b>\n\n"
            "🔔 <b>Настройки уведомлений</b> - управление оповещениями\n"
            "⏰ <b>Автозавершение аренды</b> - настройка таймеров\n"
            "🔐 <b>Безопасность</b> - управление доступом и защитой\n"
            "💾 <b>Резервное копирование</b> - настройка бэкапов\n\n"
            "⚡ Выберите категорию для настройки:"
        )
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               settings_text, settings_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data in ["rental_stats", "financial_stats", "game_stats", "popular_accounts"])
    @auth_required
    def cb_detailed_stats(call):
        """Обработчик детальной статистики"""
        bot.answer_callback_query(call.id)
        
        stat_type = call.data
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            if stat_type == "rental_stats":
                # Статистика аренды - считаем активные аренды
                c.execute("SELECT COUNT(*) FROM accounts WHERE status != 'free'")
                active_rentals = c.fetchone()[0]
                
                stats_text = (
                    f"📈 <b>Статистика аренды</b>\n\n"
                    f"🔥 <b>Активных аренд:</b> {active_rentals}\n"
                    f"⏰ <b>Система автозавершения:</b> Активна\n"
                    f"🔄 <b>Автосмена паролей:</b> Включена\n\n"
                    f"📋 Подробная статистика в разработке..."
                )
                
            elif stat_type == "game_stats":
                # Статистика по играм
                c.execute("SELECT game, COUNT(*) FROM accounts GROUP BY game ORDER BY COUNT(*) DESC")
                games_data = c.fetchall()
                
                stats_text = "🎮 <b>Статистика по играм</b>\n\n"
                for game, count in games_data:
                    emoji = get_game_emoji(game)
                    stats_text += f"{emoji} <b>{game}:</b> {count} аккаунтов\n"
                
                if not games_data:
                    stats_text += "📭 Нет данных по играм"
                    
            elif stat_type == "popular_accounts":
                # Популярные аккаунты (по игрям)
                c.execute("SELECT game, COUNT(*) as cnt FROM accounts GROUP BY game ORDER BY cnt DESC LIMIT 5")
                popular_games = c.fetchall()
                
                stats_text = "📊 <b>Популярные категории</b>\n\n"
                for i, (game, count) in enumerate(popular_games, 1):
                    emoji = get_game_emoji(game)
                    medal = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][i-1] if i <= 5 else "🔸"
                    stats_text += f"{medal} {emoji} <b>{game}:</b> {count} аккаунтов\n"
                
                if not popular_games:
                    stats_text += "📭 Нет данных"
                    
            else:  # financial_stats
                stats_text = (
                    "💰 <b>Финансовая сводка</b>\n\n"
                    "💡 <b>Функция в разработке</b>\n\n"
                    "📊 Здесь будет отображаться:\n"
                    "• Доходы от аренды\n"
                    "• Статистика платежей\n"
                    "• Популярные тарифы\n"
                    "• Средняя длительность аренды"
                )
            
            conn.close()
            
        except Exception as e:
            logger.error(f"Ошибка при получении детальной статистики {stat_type}: {e}")
            stats_text = f"❌ Ошибка при загрузке статистики {stat_type}"
        
        # Клавиатура возврата
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔙 К статистике", callback_data="stats"))
        kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               stats_text, kb, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data in ["notification_settings", "auto_end_settings", "security_settings", "backup_settings"])
    @auth_required
    def cb_settings_category(call):
        """Обработчик категорий настроек"""
        bot.answer_callback_query(call.id)
        
        setting_type = call.data
        
        if setting_type == "notification_settings":
            settings_text = (
                "🔔 <b>Настройки уведомлений</b>\n\n"
                "📬 <b>Текущие настройки:</b>\n"
                "• ✅ Уведомления о новых заказах\n"
                "• ✅ Уведомления о смене паролей\n"
                "• ✅ Уведомления об ошибках\n"
                "• ✅ Уведомления о завершении аренды\n\n"
                "⚙️ <b>Настройка каналов уведомлений в разработке</b>"
            )
            
        elif setting_type == "auto_end_settings":
            settings_text = (
                "⏰ <b>Автозавершение аренды</b>\n\n"
                "🔧 <b>Текущие настройки:</b>\n"
                "• ✅ Автозавершение включено\n"
                "• ✅ Автосмена паролей включена\n"
                "• ⏱️ Предупреждение за 10 минут до окончания\n"
                "• 🔄 Восстановление сессий при перезапуске\n\n"
                "💡 <b>Система работает автоматически</b>"
            )
            
        elif setting_type == "security_settings":
            settings_text = (
                "🔐 <b>Настройки безопасности</b>\n\n"
                "🛡️ <b>Активные меры защиты:</b>\n"
                "• ✅ Авторизация по Telegram ID\n"
                "• ✅ Логирование всех действий\n"
                "• ✅ Защищенное хранение паролей\n"
                "• ✅ Автоматические скриншоты процессов\n\n"
                "🔒 <b>Система безопасности активна</b>"
            )
            
        else:  # backup_settings
            settings_text = (
                "💾 <b>Резервное копирование</b>\n\n"
                "📁 <b>Что сохраняется:</b>\n"
                "• 🗄️ База данных аккаунтов\n"
                "• 📊 Логи системы\n"
                "• 📸 Скриншоты процессов\n"
                "• ⚙️ Настройки конфигурации\n\n"
                "🔄 <b>Автоматическое резервирование в разработке</b>"
            )
        
        # Клавиатура возврата
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("🔙 К настройкам", callback_data="settings"))
        kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               settings_text, kb, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("get_code:"))
    @auth_required
    def cb_get_code(call):
        """Обработчик для получения Steam Guard кода с почты"""
        bot.answer_callback_query(call.id, "🔍 Ищем код на почте...", show_alert=False)
        
        try:
            acc_id = call.data.split(":")[1]
            
            # Получаем данные аккаунта
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT login, email_login, email_password, imap_host FROM accounts WHERE id=?", (acc_id,))
            result = c.fetchone()
            conn.close()
            
            if not result:
                bot.send_message(call.message.chat.id, "❌ Аккаунт не найден")
                return
                
            login, email_login, email_password, imap_host = result
            
            if not (email_login and email_password and imap_host):
                bot.send_message(call.message.chat.id, 
                    f"❌ <b>Для аккаунта {login} не настроены данные почты!</b>\n\n"
                    f"📧 Настройте почту через кнопку 'Почта' в управлении аккаунтом.",
                    parse_mode="HTML")
                return
                
            # Отправляем сообщение о начале поиска
            status_msg = bot.send_message(call.message.chat.id, 
                f"🔍 <b>Поиск Steam Guard кода для аккаунта {login}</b>\n\n"
                f"📧 Подключаюсь к почте: {email_login[:3]}***@{email_login.split('@')[1]}\n"
                f"⏳ Ищу новые письма от Steam...", 
                parse_mode="HTML")
            
            # Импортируем и используем функцию получения кода
            from utils.email_utils import fetch_steam_guard_code_from_email
            import logging
            
            # Создаем логгер для отслеживания процесса
            logger = logging.getLogger(__name__)
            
            # Получаем код (таймаут 60 секунд для быстрого поиска)
            code = fetch_steam_guard_code_from_email(
                email_login=email_login,
                email_password=email_password,
                imap_host=imap_host,
                timeout=60,
                logger=logger,
                mode='login',
                force_new=True
            )
            
            if code:
                # Успешно получен код
                bot.edit_message_text(
                    f"✅ <b>Код Steam Guard найден!</b>\n\n"
                    f"🎯 Аккаунт: {login}\n"
                    f"🔑 Код: <code>{code}</code>\n\n"
                    f"📋 Нажмите на код, чтобы скопировать",
                    chat_id=call.message.chat.id,
                    message_id=status_msg.message_id,
                    parse_mode="HTML"
                )
            else:
                # Код не найден
                bot.edit_message_text(
                    f"❌ <b>Код Steam Guard не найден</b>\n\n"
                    f"🎯 Аккаунт: {login}\n"
                    f"📧 Почта: {email_login[:3]}***@{email_login.split('@')[1]}\n\n"
                    f"🔍 Возможные причины:\n"
                    f"• Нет новых писем от Steam\n"
                    f"• Неправильные настройки почты\n"
                    f"• Код уже использован\n"
                    f"• Проблемы с IMAP подключением\n\n"
                    f"💡 Попробуйте запросить новый код входа в Steam",
                    chat_id=call.message.chat.id,
                    message_id=status_msg.message_id,
                    parse_mode="HTML"
                )
                
        except Exception as e:
            logger.error(f"[GET_CODE] Ошибка при получении кода: {e}")
            bot.send_message(call.message.chat.id, 
                f"❌ <b>Ошибка при получении кода</b>\n\n"
                f"🔧 Техническая информация: {str(e)}\n\n"
                f"💡 Проверьте настройки почты аккаунта",
                parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "noop")
    def cb_noop(call):
        """Обработчик для неактивных кнопок (например, индикатор страницы)"""
        bot.answer_callback_query(call.id, "ℹ️ Это информационная кнопка", show_alert=False)

# next update