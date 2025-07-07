"""Steam operations handlers for Telegram bot"""

import sqlite3
import threading
import asyncio
import os
import html
from .base import BaseHandler
from tg_utils.db import DB_PATH
from tg_utils.state import user_states
from .utils import parse_imap_host_port
from utils.email_utils import fetch_steam_guard_code_from_email
from steam.steam_password_changer import change_steam_password


def init_steam_operations_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """Initialize Steam operations handlers"""
    
    # Create handler instance
    handler = BaseHandler(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    bot = bot_instance
    auth_required = auth_required_decorator or (lambda func: func)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("test:"))
    def cb_test_account(call):
        handler.answer_callback_query(call)
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

        bot.send_message(call.message.chat.id, f"⏳ Запускаю тест для <code>{login}</code>...", parse_mode="HTML")

        async def run_test():
            try:
                handler.log_info(f"[TEST] Начинаем тестирование аккаунта {acc_id} ({login})")
                
                from utils.browser_config import get_browser_config
                screenshots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'screenshots')
                os.makedirs(screenshots_dir, exist_ok=True)
                
                from playwright.async_api import async_playwright
                
                async with async_playwright() as p:
                    browser_config = get_browser_config()
                    browser = await p.chromium.launch(**browser_config)
                    
                    context = await browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        ignore_https_errors=True,
                        java_script_enabled=True
                    )
                    
                    page = await context.new_page()
                    handler.log_info(f"[TEST][ID: {acc_id}][LOGIN: {login}] ✅ Контекст создан")
                    
                    try:
                        # Переходим на страницу входа Steam
                        await page.goto('https://store.steampowered.com/login/', wait_for_load_state='networkidle')
                        await asyncio.sleep(2)
                        
                        # Скриншот страницы входа
                        screenshot_path = os.path.join(screenshots_dir, f'login_page_{acc_id}.png')
                        await page.screenshot(path=screenshot_path)
                        
                        # Заполняем данные входа
                        await page.fill('input[type="text"]', login)
                        await page.fill('input[type="password"]', password)
                        
                        # Нажимаем вход
                        await page.click('button[type="submit"]')
                        await page.wait_for_load_state('networkidle')
                        
                        # Ждем ответа от Steam
                        await page.wait_for_selector("#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=25000)
                        handler.log_info(f"[TEST][ID: {acc_id}][LOGIN: {login}] Получен ответ от Steam")
                        
                        # Проверка необходимости Steam Guard
                        need_guard = False
                        if await page.query_selector("#auth_buttonset_entercode"):
                            need_guard = True
                            handler.log_info(f"[TEST][ID: {acc_id}][LOGIN: {login}] Обнаружена форма Steam Guard (entercode)")
                        elif await page.query_selector("input[maxlength='1']"):
                            need_guard = True
                            handler.log_info(f"[TEST][ID: {acc_id}][LOGIN: {login}] Обнаружена форма Steam Guard (6 цифр)")
                        
                        if need_guard:
                            bot.send_message(call.message.chat.id, f"🔐 Аккаунт <code>{login}</code> требует Steam Guard код", parse_mode="HTML")
                            
                            if email_login and email_password and imap_host:
                                bot.send_message(call.message.chat.id, f"📧 Ищу код на почте...")
                                
                                # Получаем код с почты
                                imap_host_clean, imap_port_clean = parse_imap_host_port(imap_host)
                                code = None
                                
                                # Пытаемся получить код несколько раз
                                for attempt in range(3):
                                    try:
                                        code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host_clean, imap_port_clean, logger=handler.logger, mode='login')
                                        if code: 
                                            break
                                    except Exception as email_ex:
                                        handler.log_error(f"Ошибка при получении кода Steam Guard с почты: {email_ex}")
                                    await asyncio.sleep(4)

                                if code:
                                    bot.send_message(call.message.chat.id, f"🔑 Найден код: <code>{code}</code>", parse_mode="HTML")
                                    
                                    # Вводим код
                                    if await page.query_selector("input[maxlength='1']"):
                                        # 6-значный код
                                        for i, digit in enumerate(code):
                                            await page.fill(f'input[maxlength="1"]:nth-of-type({i+1})', digit)
                                    else:
                                        # Обычное поле ввода
                                        await page.fill('input[name="authcode"]', code)
                                    
                                    await page.click('button:has-text("Continue"), input[type="submit"]')
                                    await page.wait_for_load_state('networkidle')
                                    await asyncio.sleep(3)
                                else:
                                    bot.send_message(call.message.chat.id, f"❌ Код не найден на почте для <code>{login}</code>", parse_mode="HTML")
                                    return
                            else:
                                bot.send_message(call.message.chat.id, f"❌ Данные почты не настроены для <code>{login}</code>", parse_mode="HTML")
                                return

                        # Проверяем успешность входа
                        if await page.query_selector("#account_pulldown"):
                            # Успешный вход
                            screenshot_path = os.path.join(screenshots_dir, f'success_{acc_id}.png')
                            await page.screenshot(path=screenshot_path)
                            
                            # Отправляем результат
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo, 
                                    caption=f"✅ <b>Тест пройден успешно!</b>\n\n"
                                           f"🎮 Аккаунт: <code>{html.escape(login)}</code>\n"
                                           f"🔑 Пароль: <code>{html.escape(password)}</code>\n"
                                           f"🟢 Статус: Вход выполнен", 
                                    parse_mode="HTML")
                        else:
                            # Неудачный вход
                            error_element = await page.query_selector(".newlogindialog_FormError")
                            error_text = await error_element.text_content() if error_element else "Неизвестная ошибка"
                            
                            screenshot_path = os.path.join(screenshots_dir, f'error_{acc_id}.png')
                            await page.screenshot(path=screenshot_path)
                            
                            with open(screenshot_path, 'rb') as photo:
                                bot.send_photo(call.message.chat.id, photo,
                                    caption=f"❌ <b>Тест не пройден</b>\n\n"
                                           f"🎮 Аккаунт: <code>{html.escape(login)}</code>\n"
                                           f"🔴 Ошибка: {html.escape(error_text)}", 
                                    parse_mode="HTML")
                                    
                    except Exception as test_error:
                        handler.log_error(f"Ошибка при тестировании аккаунта {acc_id}", test_error)
                        bot.send_message(call.message.chat.id, f"❌ Ошибка при тестировании: {test_error}")
                    finally:
                        await browser.close()
                        
            except Exception as e:
                handler.log_error(f"Критическая ошибка при тестировании аккаунта {acc_id}", e)
                bot.send_message(call.message.chat.id, f"❌ Критическая ошибка: {e}")

        def worker():
            asyncio.run(run_test())

        threading.Thread(target=worker).start()

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
            handler.answer_callback_query(call, "❌ Нет данных почты")
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
                handler.log_error(f"Ошибка при получении кода: {e}")
                bot.send_message(call.message.chat.id, f"❌ Ошибка: {e}")
                
        threading.Thread(target=get_guard_code).start()
        handler.answer_callback_query(call, "⏳ Получаем код...")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("get_code:"))
    @auth_required
    def cb_get_code(call):
        """Обработчик для получения Steam Guard кода с почты"""
        handler.answer_callback_query(call, "🔍 Ищем код на почте...", False)
        
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
            
            def search_code():
                try:
                    # Парсим IMAP данные
                    imap_host_clean, imap_port_clean = parse_imap_host_port(imap_host)
                    
                    # Получаем код (таймаут 60 секунд для быстрого поиска)
                    code = fetch_steam_guard_code_from_email(
                        email_login=email_login,
                        email_password=email_password,
                        imap_host=imap_host_clean,
                        imap_port=imap_port_clean,
                        logger=handler.logger,
                        mode='any',
                        timeout=60
                    )
                    
                    if code:
                        # Код найден
                        bot.edit_message_text(
                            f"✅ <b>Steam Guard код найден!</b>\n\n"
                            f"🎮 Аккаунт: <code>{login}</code>\n"
                            f"🔑 Код: <code>{code}</code>\n\n"
                            f"📋 Скопируйте код и используйте его для входа в Steam",
                            chat_id=call.message.chat.id,
                            message_id=status_msg.message_id,
                            parse_mode="HTML"
                        )
                    else:
                        # Код не найден
                        bot.edit_message_text(
                            f"❌ <b>Steam Guard код не найден</b>\n\n"
                            f"🎮 Аккаунт: <code>{login}</code>\n\n"
                            f"📧 Возможные причины:\n"
                            f"• Письмо еще не пришло (попробуйте через 1-2 минуты)\n"
                            f"• Проблемы с почтовыми настройками\n"
                            f"• Блокировка IMAP на почтовом сервере",
                            chat_id=call.message.chat.id,
                            message_id=status_msg.message_id,
                            parse_mode="HTML"
                        )
                        
                except Exception as e:
                    handler.log_error(f"Ошибка поиска кода для аккаунта {acc_id}", e)
                    bot.edit_message_text(
                        f"❌ <b>Ошибка при поиске кода</b>\n\n"
                        f"🎮 Аккаунт: <code>{login}</code>\n"
                        f"🚫 Ошибка: {str(e)}\n\n"
                        f"🔧 Проверьте настройки почты аккаунта",
                        chat_id=call.message.chat.id,
                        message_id=status_msg.message_id,
                        parse_mode="HTML"
                    )
            
            # Запускаем поиск в отдельном потоке
            threading.Thread(target=search_code).start()
            
        except Exception as e:
            handler.log_error(f"Ошибка в cb_get_code", e)
            bot.send_message(call.message.chat.id, f"❌ Произошла ошибка: {str(e)}")

    @bot.callback_query_handler(func=lambda c: c.data.startswith("chgdata:"))
    @auth_required
    def cb_change_data(call):
        """Handler for changing Steam account password using steam_password_changer.py"""
        handler.answer_callback_query(call)
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
        
        if not (email_login and email_password and imap_host):
            bot.send_message(call.message.chat.id, 
                f"❌ <b>Для аккаунта {login} не настроены данные почты!</b>\n\n"
                f"📧 Настройте почту через кнопку 'Почта' в управлении аккаунтом.",
                parse_mode="HTML")
            return
        
        bot.send_message(call.message.chat.id, f"⏳ Запускаю процесс смены данных для <code>{login}</code>...", parse_mode="HTML")
    
        def worker():
            async def run_change():
                try:
                    handler.log_info(f"[PASSWORD_CHANGE] Начинаем процесс смены пароля для аккаунта {acc_id} ({login})")
                    
                    from utils.browser_config import get_browser_config
                    from playwright.async_api import async_playwright
                    
                    async with async_playwright() as p:
                        browser_config = get_browser_config()
                        browser = await p.chromium.launch(**browser_config)
                        
                        context = await browser.new_context(
                            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                            ignore_https_errors=True,
                            java_script_enabled=True
                        )
                        
                        page = await context.new_page()
                        
                        try:
                            # Переходим на страницу входа Steam
                            await page.goto('https://store.steampowered.com/login/', wait_for_load_state='networkidle')
                            await asyncio.sleep(2)
                            
                            # Заполняем данные входа
                            await page.fill('input[type="text"]', login)
                            await page.fill('input[type="password"]', password)
                            
                            # Нажимаем вход
                            await page.click('button[type="submit"]')
                            await page.wait_for_load_state('networkidle')
                            
                            # Ждем ответа от Steam и обрабатываем Steam Guard если нужно
                            await page.wait_for_selector("#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=25000)
                            
                            # Проверка Steam Guard и обработка кода как в тесте
                            need_guard = False
                            if await page.query_selector("#auth_buttonset_entercode") or await page.query_selector("input[maxlength='1']"):
                                need_guard = True
                                
                            if need_guard:
                                imap_host_clean, imap_port_clean = parse_imap_host_port(imap_host)
                                code = None
                                
                                # Пытаемся получить код несколько раз
                                for attempt in range(3):
                                    try:
                                        code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host_clean, imap_port_clean, logger=handler.logger, mode='login')
                                        if code: 
                                            break
                                    except Exception as email_ex:
                                        handler.log_error(f"Ошибка при получении кода Steam Guard: {email_ex}")
                                    await asyncio.sleep(4)

                                if code:
                                    # Вводим код
                                    if await page.query_selector("input[maxlength='1']"):
                                        # 6-значный код
                                        for i, digit in enumerate(code):
                                            await page.fill(f'input[maxlength="1"]:nth-of-type({i+1})', digit)
                                    else:
                                        # Обычное поле ввода
                                        await page.fill('input[name="authcode"]', code)
                                    
                                    await page.click('button:has-text("Continue"), input[type="submit"]')
                                    await page.wait_for_load_state('networkidle')
                                    await asyncio.sleep(3)
                                else:
                                    bot.send_message(call.message.chat.id, f"❌ Не удалось получить Steam Guard код для <code>{login}</code>", parse_mode="HTML")
                                    return
                            
                            # Проверяем успешность входа
                            if not await page.query_selector("#account_pulldown"):
                                bot.send_message(call.message.chat.id, f"❌ Не удалось войти в аккаунт <code>{login}</code>", parse_mode="HTML")
                                return
                            
                            # Теперь используем унифицированную функцию смены пароля
                            def log_callback(message):
                                """Callback для отправки логов в Telegram"""
                                try:
                                    bot.send_message(call.message.chat.id, message)
                                except:
                                    pass
                            
                            # Вызываем функцию смены пароля из steam_password_changer.py
                            logs, screenshots, success = await change_steam_password(
                                context=context,
                                page=page,
                                email_login=email_login,
                                email_password=email_password,
                                imap_host=imap_host,
                                acc_id=acc_id,
                                log_callback=log_callback
                            )
                            
                            # Отправляем результат
                            if success:
                                bot.send_message(call.message.chat.id, 
                                    f"✅ <b>Смена пароля для аккаунта {login} завершена успешно!</b>", 
                                    parse_mode="HTML")
                            else:
                                bot.send_message(call.message.chat.id, 
                                    f"❌ <b>Смена пароля для аккаунта {login} не удалась</b>", 
                                    parse_mode="HTML")
                            
                            # Отправляем скриншоты если есть
                            for screenshot_path in screenshots:
                                try:
                                    if os.path.exists(screenshot_path):
                                        with open(screenshot_path, 'rb') as photo:
                                            bot.send_photo(call.message.chat.id, photo)
                                except Exception as e:
                                    handler.log_error(f"Ошибка отправки скриншота", e)
                                    
                        except Exception as change_error:
                            handler.log_error(f"Ошибка при смене пароля аккаунта {acc_id}", change_error)
                            bot.send_message(call.message.chat.id, f"❌ Ошибка при смене пароля: {change_error}")
                        finally:
                            await browser.close()
                            
                except Exception as e:
                    handler.log_error(f"Критическая ошибка при смене пароля аккаунта {acc_id}", e)
                    bot.send_message(call.message.chat.id, f"❌ Критическая ошибка: {e}")

            asyncio.run(run_change())

        threading.Thread(target=worker).start()