# NOTE next test 50
from playwright.sync_api import TimeoutError as PWTimeoutError
import os

def steam_playwright_login_and_save_session(
    browser, login, password, email_login, email_password, imap_host, session_file, logger=None,
    user_agent=None, locale=None
):
    """
    Универсальная функция playwright-логина как при нажатии кнопки 'Тест'.
    Возвращает: context, page, logs, used_session
    """
    from utils.email_utils import fetch_steam_guard_code_from_email
    logs = []
    context = None
    page = None
    used_session = False
    
    # Проверяем наличие сохраненной сессии
    if os.path.exists(session_file):
        try:
            context = browser.new_context(storage_state=session_file)
            page = context.new_page()
            page.goto("https://store.steampowered.com/account/")
            page.wait_for_selector("#account_pulldown", timeout=10000)
            used_session = True
            logs.append(f"[STEAM-SESSION] Использована сессия: {session_file}")
            return context, page, logs, used_session
        except Exception:
            if context:
                context.close()
            context = None
            logs.append(f"[STEAM-SESSION] Не удалось использовать сессию: {session_file}")
    
    # Создаем новый контекст
    if logger:
        logger.info(f"[PLAYWRIGHT_LOGIN] user_agent={user_agent or 'default'}, locale={locale or 'default'}")
    
    try:
        context = browser.new_context(
            user_agent=user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport=None,
            locale=locale or "ru-RU",
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = context.new_page()
        page.goto("https://store.steampowered.com/login/")
        page.wait_for_load_state("networkidle")
        
        # Заполнение формы логина
        try:
            page.wait_for_selector('input[type="text"]', timeout=20000)
            page.fill('input[type="text"]', login)
            page.fill('input[type="password"]', password)
            page.click("button[type='submit']")
            page.wait_for_selector("#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=25000)
            
            # Проверка необходимости ввода Steam Guard
            need_guard = False
            if page.query_selector("#auth_buttonset_entercode"):
                need_guard = True
            elif page.query_selector("input[maxlength='1']"):
                need_guard = True
            elif "Введите код, полученный на электронный адрес" in page.content():
                need_guard = True
                
            # Обработка ввода Steam Guard кода если нужно
            if need_guard:
                logs.append("[STEAM][INFO] Требуется ввод Steam Guard кода!")
                
                def save_debug_artifacts(page, login, session_file, tag, logger=None):
                    try:
                        base_dir = os.path.abspath(os.path.dirname(session_file))
                        png_path = os.path.join(base_dir, f"{tag}_{login}.png")
                        html_path = os.path.join(base_dir, f"{tag}_{login}.html")
                        page.screenshot(path=png_path)
                        with open(html_path, "w", encoding="utf-8") as f:
                            f.write(page.content())
                        if logger:
                            logger.error(f"[STEAM][SCREENSHOT] Сохранён скриншот: {png_path}")
                            logger.error(f"[STEAM][HTML] Сохранён HTML: {html_path}")
                    except Exception as ex:
                        if logger:
                            logger.error(f"[STEAM][ERROR] Не удалось сохранить скриншот/HTML: {ex}")
                
                # Проверка наличия данных почты
                if not (email_login and email_password and imap_host):
                    logs.append("[STEAM][ERROR] Для этого аккаунта не настроена почта!")
                    if page:
                        save_debug_artifacts(page, login, session_file, "steam_guard_fail", logger)
                    return None, page, logs, used_session
                
                # Получение кода Steam Guard с почты
                import time
                code = None
                start = time.time()
                logs.append("[STEAM][WAIT] Ожидание Steam Guard кода с почты (до 60 секунд)...")
                
                while time.time() - start < 60:
                    # Ищем код в письмах
                    code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, logger=logger, mode='login')
                    if code:
                        break
                    time.sleep(2)
                    
                if not code:
                    logs.append("[STEAM][ERROR] Не удалось получить код Steam Guard с почты за 60 секунд! Ожидается ручной ввод или повторная попытка.")
                    if page:
                        save_debug_artifacts(page, login, session_file, "steam_guard_fail", logger)
                    return None, page, logs, used_session
                
                # Ввод полученного кода
                logs.append(f"[STEAM][INFO] Ввожу код: {code}")
                import time
                try:
                    # Добавляем задержку перед вводом кода
                    time.sleep(1)
                    
                    # Ждём завершения сетевых запросов
                    try:
                        page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception as e:
                        logs.append(f"[STEAM][WARNING] Ожидание networkidle не завершилось: {e}")
                    
                    # Ввод кода в соответствующие поля
                    if page.query_selector("input[maxlength='1']"):
                        # Ждём появления полей для ввода кода
                        page.wait_for_selector("input[maxlength='1']", timeout=10000)
                        inputs = page.query_selector_all("input[maxlength='1']")
                        logs.append(f"[STEAM][DEBUG] Найдено инпутов для кода: {len(inputs)}. Код: {code}")
                        
                        if len(inputs) == len(code):
                            # Вводим каждый символ с небольшой задержкой
                            for i, ch in enumerate(code):
                                inputs[i].fill(ch)
                                time.sleep(0.2)
                            
                            # Ждём после ввода кода
                            time.sleep(2)
                            
                            # Нажимаем кнопку отправки если она есть
                            btn = page.query_selector("button[type='submit']")
                            if btn:
                                logs.append("[STEAM][INFO] Нажимаю кнопку отправки кода")
                                btn.click()
                        else:
                            logs.append("[STEAM][ERROR] Количество инпутов для кода не совпадает с длиной кода!")
                            if page:
                                save_debug_artifacts(page, login, session_file, "steam_guard_code_input_fail", logger)
                            return None, page, logs, used_session
                    
                    # Альтернативный способ ввода кода
                    elif page.query_selector("input[name='authcode']"):
                        logs.append("[STEAM][INFO] Ввожу код в единое поле authcode")
                        page.fill("input[name='authcode']", code)
                        time.sleep(1)
                        
                        # Нажимаем кнопку отправки
                        btn = page.query_selector("button[type='submit']")
                        if btn:
                            logs.append("[STEAM][INFO] Нажимаю кнопку отправки кода")
                            btn.click()
                    else:
                        logs.append("[STEAM][ERROR] Не найдено поле для ввода Steam Guard!")
                        if page:
                            save_debug_artifacts(page, login, session_file, "steam_guard_code_input_fail", logger)
                        return None, page, logs, used_session
                    
                    # Ждём завершения сетевых запросов после ввода кода
                    try:
                        logs.append("[STEAM][INFO] Ожидание завершения сетевых запросов после ввода кода...")
                        page.wait_for_load_state("networkidle", timeout=10000)
                        logs.append("[STEAM][INFO] Сетевые запросы завершены")
                    except Exception as e:
                        logs.append(f"[STEAM][WARNING] Ожидание networkidle после ввода кода не завершилось: {e}")
                    
                    # Дополнительная задержка для обработки кода
                    time.sleep(3)
                    
                except Exception as ex:
                    logs.append(f"[STEAM][ERROR] Ошибка при вводе Steam Guard кода: {ex}")
                    if page:
                        save_debug_artifacts(page, login, session_file, "steam_guard_code_input_fail", logger)
                    return None, page, logs, used_session
            
            # Проверка успешности входа
            try:
                import time
                # Даём странице время загрузиться
                time.sleep(3)
                
                # Ждём завершения сетевых запросов
                try:
                    page.wait_for_load_state("networkidle", timeout=5000)
                except Exception as e:
                    logs.append(f"[STEAM][WARNING] Ожидание networkidle не завершилось: {e}")
                
                # Проверяем наличие признаков успешного входа
                if page.query_selector("#account_pulldown"):
                    try:
                        # Сохраняем сессию после успешного входа
                        logs.append("[STEAM-SESSION] Пытаюсь сохранить сессию...")
                        time.sleep(1)
                        context.storage_state(path=session_file)
                        logs.append(f"[STEAM-SESSION] Сессия успешно сохранена: {session_file}")
                    except Exception as ex:
                        logs.append(f"[STEAM-SESSION] Не удалось сохранить storage_state: {ex}")
                    
                    logs.append("[STEAM][SUCCESS] Вход выполнен!")
                    time.sleep(2)
                    return context, page, logs, used_session
            except Exception as ex:
                logs.append(f"[STEAM][ERROR] Ошибка при проверке успешного входа: {ex}")
                # Альтернативная проверка на успешный вход
                if page and page.content() and "#account_pulldown" in page.content():
                    logs.append("[STEAM][SUCCESS] Вход выполнен (alternative check)!")
                    try:
                        context.storage_state(path=session_file)
                        logs.append(f"[STEAM-SESSION] Сессия сохранена (alternative method): {session_file}")
                    except Exception as ex2:
                        logs.append(f"[STEAM-SESSION] Не удалось сохранить storage_state (alternative method): {ex2}")
                    return context, page, logs, used_session
            
            # Проверка наличия сообщения об ошибке
            if page and page.query_selector(".newlogindialog_FormError"):
                err = page.inner_text(".newlogindialog_FormError")
                logs.append(f"[STEAM][ERROR] Ошибка входа: {err}")
                try:
                    page.screenshot(path=f"steam_login_error_{login}.png")
                    with open(f"steam_login_error_{login}.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                except Exception as ex:
                    logs.append(f"[STEAM][ERROR] Не удалось сохранить скриншот/HTML: {ex}")
                return None, page, logs, used_session
            
            # Если не удалось определить результат
            logs.append("[STEAM][ERROR] Не удалось определить результат после логина.")
            try:
                page.screenshot(path=f"steam_login_unknown_{login}.png")
                with open(f"steam_login_unknown_{login}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception as ex:
                logs.append(f"[STEAM][ERROR] Не удалось сохранить скриншот/HTML: {ex}")
            return None, page, logs, used_session
            
        except Exception as form_ex:
            logs.append(f"[STEAM][ERROR] Ошибка при заполнении формы логина: {form_ex}")
            return None, None, logs, used_session
            
    except Exception as e:
        logs.append(f"[STEAM][ERROR] Ошибка playwright-логина: {e}")
        if page:
            try:
                page.screenshot(path=f"steam_login_exception_{login}.png")
                with open(f"steam_login_exception_{login}.html", "w", encoding="utf-8") as f:
                    f.write(page.content())
            except Exception as ex:
                logs.append(f"[STEAM][ERROR] Не удалось сохранить скриншот/HTML: {ex}")
        return None, page, logs, used_session
