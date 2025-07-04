import asyncio
import os
import secrets
import string
import time
from utils.password import generate_password
from db.accounts import update_account_password
from utils.email_utils import fetch_steam_guard_code_from_email
from utils.logger import logger

async def clear_session(page, acc_id):
    """Полная очистка сессии браузера"""
    logger.info(f"[STEAM][ID: {acc_id}] Начинаю очистку сессии...")
    
    # Очищаем cookies
    await page.context.clear_cookies()
    logger.info(f"[STEAM][ID: {acc_id}] Cookies очищены")
    
    # Очищаем localStorage и sessionStorage
    await page.evaluate("""
        () => {
            try {
                localStorage.clear();
                sessionStorage.clear();
                if ('indexedDB' in window) {
                    indexedDB.deleteDatabase('steam');
                }
            } catch (e) {
                console.log('Ошибка очистки storage:', e);
            }
        }
    """)
    logger.info(f"[STEAM][ID: {acc_id}] LocalStorage и SessionStorage очищены")
    
    # Перезагружаем страницу
    response = await page.reload(wait_until='networkidle')
    logger.info(f"[STEAM][ID: {acc_id}] Страница перезагружена")

async def log_page_state(page, acc_id, step_name):
    """Логирование состояния страницы"""
    url = page.url
    title = await page.title()
    logger.info(f"[STEAM][ID: {acc_id}][{step_name}] URL: {url}")
    logger.info(f"[STEAM][ID: {acc_id}][{step_name}] Заголовок: {title}")

async def find_and_click_element(page, selectors, acc_id, action_name):
    """Поиск и клик по элементу с множественными селекторами"""
    logger.info(f"[STEAM][ID: {acc_id}] Ищем элемент для: {action_name}")
    
    for i, selector in enumerate(selectors):
        logger.debug(f"[STEAM][ID: {acc_id}] Пробуем селектор {i+1}/{len(selectors)}: {selector}")
        
        element = await page.query_selector(selector)
        if element:
            visible = await element.is_visible()
            logger.info(f"[STEAM][ID: {acc_id}] Элемент найден: видим={visible}")
            
            if visible:
                await element.click()
                logger.info(f"[STEAM][ID: {acc_id}] ✅ Клик выполнен по селектору: {selector}")
                return True
        else:
            logger.debug(f"[STEAM][ID: {acc_id}] Селектор не найден: {selector}")
    
    logger.error(f"[STEAM][ID: {acc_id}] ❌ Не удалось найти элемент для: {action_name}")
    return False

async def fill_input_safely(page, selectors, value, acc_id, field_name):
    """Безопасное заполнение поля ввода"""
    logger.info(f"[STEAM][ID: {acc_id}] Заполняем поле: {field_name}")
    
    for selector in selectors:
        logger.debug(f"[STEAM][ID: {acc_id}] Пробуем селектор: {selector}")
        
        element = await page.query_selector(selector)
        if element:
            visible = await element.is_visible()
            if visible:
                await element.fill("")  # Очищаем
                await element.fill(value)  # Заполняем
                
                # Проверяем
                entered_value = await element.input_value()
                success = entered_value == value
                
                logger.info(f"[STEAM][ID: {acc_id}] Заполнение {field_name}: успех={success}")
                if success:
                    return True
    
    logger.error(f"[STEAM][ID: {acc_id}] ❌ Не удалось заполнить поле: {field_name}")
    return False

async def check_if_reauth_required(page, logs):
    """
    Проверяет, требуется ли повторная авторизация в Steam
    
    Args:
        page: Playwright page
        logs: список логов для записи
    
    Returns:
        bool: True если требуется повторная авторизация, False если нет
    """
    try:
        # Проверяем URL - если мы на странице входа, значит нужна повторная авторизация
        current_url = page.url
        if 'login' in current_url.lower() or 'signin' in current_url.lower():
            logs.append(f"[STEAM][REAUTH] Обнаружен переход на страницу входа: {current_url}")
            return True
            
        # Проверяем наличие полей входа
        login_selectors = [
            'input[name="username"]',
            'input[name="password"]',
            '#input_username',
            '#input_password',
            '.login_form',
            'form#loginForm'
        ]
        
        for selector in login_selectors:
            try:
                element = await page.query_selector(selector)
                if element:
                    logs.append(f"[STEAM][REAUTH] Найден элемент входа: {selector}")
                    return True
            except:
                continue
                
        # Проверяем текст на странице
        page_content = await page.content()
        reauth_indicators = [
            'sign in',
            'log in',
            'войти',
            'авторизация',
            'пароль неверен',
            'session expired',
            'сессия истекла'
        ]
        
        page_text = page_content.lower()
        for indicator in reauth_indicators:
            if indicator in page_text:
                logs.append(f"[STEAM][REAUTH] Найден индикатор повторной авторизации: {indicator}")
                return True
                
        return False
        
    except Exception as e:
        logs.append(f"[STEAM][REAUTH][ERROR] Ошибка при проверке повторной авторизации: {e}")
        return False

async def change_steam_password(context, page, email_login, email_password, imap_host, acc_id, log_callback=None):
    """
    Функция для смены пароля в Steam аккаунте
    
    Args:
        context: Playwright context, должен быть валидным и авторизованным в Steam
        page: Playwright page, текущая страница
        email_login: логин от почты
        email_password: пароль от почты
        imap_host: IMAP хост почты
        acc_id: ID аккаунта в базе данных
        log_callback: функция обратного вызова для логирования
        
    Returns:
        tuple: (logs, screenshots, success) - список логов, список скриншотов, флаг успеха
    """
    logs = []
    screenshots = []
    success = False
    
    try:
        logs.append(f"[STEAM][ID: {acc_id}] Начинаем смену данных")
        logger.info(f"[STEAM][ID: {acc_id}] Запуск функции смены пароля")
        
        # ОЧИСТКА СЕССИИ ПЕРЕД НАЧАЛОМ
        await clear_session(page, acc_id)
        
        # Переход на страницу аккаунта
        logger.info(f"[STEAM][ID: {acc_id}] Переходим на страницу аккаунта")
        response = await page.goto("https://store.steampowered.com/account/", wait_until='networkidle')
        status = response.status if response else 0
        logger.info(f"[STEAM][ID: {acc_id}] Переход завершен, статус: {status}")
        
        await asyncio.sleep(2)
        await log_page_state(page, acc_id, "АККАУНТ")

        # Создаем директорию для скриншотов если её нет
        screenshots_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'screenshots')
        os.makedirs(screenshots_dir, exist_ok=True)
        
        # Сохраняем скриншот начальной страницы
        screenshot_path = os.path.join(screenshots_dir, f"account_step1_profile_{acc_id}.png")
        await page.screenshot(path=screenshot_path)
        screenshots.append(screenshot_path)
        
        # Вместо поиска и клика по 'Сменить пароль', переходим напрямую по URL
        # Переход на страницу смены пароля с улучшенными проверками
        logger.info(f"[STEAM][ID: {acc_id}] Переходим на страницу смены пароля")
        response = await page.goto("https://help.steampowered.com/wizard/HelpChangePassword?redir=store/account/", wait_until='networkidle')
        status = response.status if response else 0

        if status == 200:
            logs.append(f"[STEAM][ID: {acc_id}] ✅ Перешли на страницу смены пароля (статус: {status})")
            logger.info(f"[STEAM][ID: {acc_id}] Успешный переход на страницу смены пароля")
        else:
            logs.append(f"[STEAM][ID: {acc_id}] ❌ Ошибка перехода на страницу смены пароля (статус: {status})")
            logger.error(f"[STEAM][ID: {acc_id}] Неудачный переход, статус: {status}")
            screenshot_path = os.path.join(screenshots_dir, f"account_fail_chgpass_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False

        await asyncio.sleep(2)
        await log_page_state(page, acc_id, "СМЕНА_ПАРОЛЯ")
            
        await asyncio.sleep(2)
        
        # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем, не требуется ли повторная авторизация
        if await check_if_reauth_required(page, logs):
            logs.append("[STEAM][ERROR] Steam запросил повторную авторизацию! Процесс смены пароля прерван.")
            screenshot_path = os.path.join(screenshots_dir, f"account_reauth_required_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False
            
        screenshot_path = os.path.join(screenshots_dir, f"account_step2_help_{acc_id}.png")
        await page.screenshot(path=screenshot_path)
        screenshots.append(screenshot_path)
        
        # Кликаем по 'Отправить подтверждение на почту'
        try:
            await page.click('a.help_wizard_button')
            logs.append("[STEAM] Кликнули по 'Отправить подтверждение'.")
        except Exception as e:
            logs.append(f"[STEAM][ERROR] Не удалось найти/нажать 'Отправить подтверждение': {e}")
            screenshot_path = os.path.join(screenshots_dir, f"account_fail_sendcode_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False
            
        await asyncio.sleep(2)
        screenshot_path = os.path.join(screenshots_dir, f"account_step3_waitcode_{acc_id}.png")
        await page.screenshot(path=screenshot_path)
        screenshots.append(screenshot_path)
        
        # Получаем код с почты через IMAP
        logs.append("[STEAM] Ожидание кода с почты...")
        code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, logger=logger, mode='change')
        if not code:
            logs.append("[STEAM][ERROR] Не удалось получить код с почты!")
            return logs, screenshots, False
            
        logs.append(f"[STEAM] Код получен: {code}")
        
        # Вводим код в форму для смены данных
        code_selectors = [
            'input[type="text"]',
            'input[name="authcode"]',
            'input[placeholder*="code"]',
            'input[placeholder*="код"]',
            '.authcode_entry input'
        ]

        code_filled = await fill_input_safely(page, code_selectors, code, acc_id, "код подтверждения")

        if code_filled:
            logs.append(f"[STEAM][ID: {acc_id}] ✅ Код введен: {code}")
            logger.info(f"[STEAM][ID: {acc_id}] Успешный ввод кода")
        else:
            logs.append(f"[STEAM][ID: {acc_id}] ❌ Не удалось ввести код")
            logger.error(f"[STEAM][ID: {acc_id}] Неудачный ввод кода")
            screenshot_path = os.path.join(screenshots_dir, f"account_fail_code_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False

            
        await asyncio.sleep(1)
        await page.click('button:has-text("Continue"), input[type="submit"]')
        logs.append("[STEAM] Нажали 'Continue' после ввода кода.")
        await asyncio.sleep(3)
        
        # КРИТИЧЕСКАЯ ПРОВЕРКА: проверяем еще раз, не запросил ли Steam повторную авторизацию
        if await check_if_reauth_required(page, logs):
            logs.append("[STEAM][ERROR] Steam запросил повторную авторизацию после ввода кода! Прерываем процесс.")
            screenshot_path = os.path.join(screenshots_dir, f"account_reauth_after_code_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False
        
        screenshot_path = os.path.join(screenshots_dir, f"account_step5_submitted_{acc_id}.png")
        await page.screenshot(path=screenshot_path)
        screenshots.append(screenshot_path)
        logs.append("[STEAM] Первый этап смены пароля завершен. Переходим к смене пароля.")

        # Генерируем новый пароль
        import secrets
        import string

        # Определяем наборы символов
        lower = string.ascii_lowercase
        upper = string.ascii_uppercase
        digits = string.digits

        # Гарантируем наличие как минимум одной буквы в верхнем регистре, одной в нижнем и одной цифры
        password_chars = [
            secrets.choice(lower),
            secrets.choice(upper),
            secrets.choice(digits)
        ]

        # Остальные символы заполняем случайным выбором из всех трех наборов
        all_chars = lower + upper + digits
        for _ in range(16 - len(password_chars)): # Длина пароля 16 символов
            password_chars.append(secrets.choice(all_chars))

        # Перемешиваем символы, чтобы пароль был случайным
        secrets.SystemRandom().shuffle(password_chars)
        new_password = ''.join(password_chars)

        logs.append(f"[STEAM] Новый сгенерированный пароль: {new_password}")
        
        try:
            await asyncio.sleep(2)
            
            # КРИТИЧЕСКАЯ ПРОВЕРКА: убеждаемся, что мы не попали на страницу входа
            if await check_if_reauth_required(page, logs):
                logs.append("[STEAM][ERROR] Steam запросил повторную авторизацию на этапе смены пароля! Прерываем процесс.")
                screenshot_path = os.path.join(screenshots_dir, f"account_reauth_before_password_{acc_id}.png")
                await page.screenshot(path=screenshot_path)
                screenshots.append(screenshot_path)
                return logs, screenshots, False
            
            # Ждём появления хотя бы одного поля для пароля
            await page.wait_for_selector('input[type="password"]', timeout=15000)
            
            screenshot_path = os.path.join(screenshots_dir, f"account_step6_before_fill_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            
            # Пробуем разные варианты селекторов
            password_fields = await page.query_selector_all('input[type="password"]')
            if len(password_fields) >= 2:
                await password_fields[0].fill(new_password)
                await password_fields[1].fill(new_password)
                logs.append("[STEAM] Ввели новый пароль в оба поля (по порядку).")
            else:
                # Пробуем по placeholder/name
                filled = False
                for sel in [
                    'input[placeholder*="Change my password"]',
                    'input[placeholder*="Re-enter"]',
                    'input[name*="new_password"]',
                    'input[name*="reenter"]'
                ]:
                    try:
                        await page.fill(sel, new_password)
                        logs.append(f"[STEAM] Ввели новый пароль в поле {sel}.")
                        filled = True
                    except Exception:
                        pass
                        
                if not filled:
                    logs.append("[STEAM][ERROR] Не удалось найти оба поля для ввода нового пароля!")
                    screenshot_path = os.path.join(screenshots_dir, f"account_fail_nopassfields_{acc_id}.png")
                    await page.screenshot(path=screenshot_path)
                    screenshots.append(screenshot_path)
                    return logs, screenshots, False
                    
            await asyncio.sleep(1)
            screenshot_path = os.path.join(screenshots_dir, f"account_step6_filled_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            
            # Ждём, чтобы кнопка стала активной
            try:
                await page.wait_for_selector('button:has-text("Change Password"):not([disabled])', timeout=15000)
                logs.append("[STEAM] Кнопка 'Change Password' стала активной.")
            except Exception:
                logs.append("[STEAM][WARNING] Кнопка 'Change Password' не стала активной за 15 секунд. Пробуем кликать всё равно...")
                
            screenshot_path = os.path.join(screenshots_dir, f"account_step6_before_click_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            
            # Пробуем разные варианты селекторов для кнопки
            clicked = False
            for sel in [
                'button:has-text("Change Password"):not([disabled])',
                'button:has-text("Change Password")',
                'button[type="submit"]',
                'input[type="submit"]',
            ]:
                try:
                    await page.click(sel, timeout=3000)
                    logs.append(f"[STEAM] Нажали на кнопку смены пароля через селектор: {sel}")
                    clicked = True
                    break
                except Exception:
                    continue
                    
            if not clicked:
                logs.append("[STEAM][ERROR] Не удалось нажать на кнопку смены пароля ни одним из селекторов!")
                screenshot_path = os.path.join(screenshots_dir, f"account_fail_changepass_btn_{acc_id}.png")
                await page.screenshot(path=screenshot_path)
                screenshots.append(screenshot_path)
                return logs, screenshots, False
                
            logs.append("[STEAM] Нажали 'Change Password'. Ожидаем результат...")
            await asyncio.sleep(4)
            
            screenshot_path = os.path.join(screenshots_dir, f"account_step7_result_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            
            # Проверяем успешность смены пароля
            content = await page.content()
            if "success" in content.lower() or "пароль успешно" in content.lower():
                logs.append(f"[STEAM][SUCCESS] Пароль успешно изменён! Новый пароль: {new_password}")
                # Обновить пароль в базе
                await update_account_password(acc_id, new_password)
                logs.append(f"[STEAM][SUCCESS] Данные аккаунта обновлены в БД: {acc_id}, {new_password[:3]}***")
                success = True
            else:
                logs.append("[STEAM][WARNING] Не удалось однозначно определить успешную смену пароля. Проверьте скриншоты!")
                
        except Exception as e:
            logs.append(f"[STEAM][ERROR] Ошибка при смене пароля: {e}")
            screenshot_path = os.path.join(screenshots_dir, f"account_fail_changepass_{acc_id}.png")
            await page.screenshot(path=screenshot_path)
            screenshots.append(screenshot_path)
            return logs, screenshots, False
    except Exception as e:
        logs.append(f"[STEAM][ERROR] {e}")
        
    # Вызываем callback если он есть
    if log_callback:
        for log in logs:
            log_callback(log)
            
    # Возвращаем 3 значения
    return logs, screenshots, success
