import time
import sqlite3
from datetime import datetime, timedelta
from typing import Optional, Tuple, Callable
from tg_utils.db import DB_PATH
from tg_utils.logger import logger

# Попытаемся импортировать pytz напрямую из виртуальной среды
try:
    import os
    import sys
    import site
    
    # Устанавливаем кодировку вывода для поддержки UTF-8, особенно для Windows
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception as e:
        # Логируем ошибку, но не прерываем выполнение
        logger.warning(f"Не удалось перенастроить кодировку sys.stdout/stderr: {e}")

    # Получаем пути к site-packages
    site_packages = site.getsitepackages()
    venv_site_packages = None
    
    # Проверяем наличие virtualenv
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        venv_path = sys.prefix
        venv_site_packages = os.path.join(venv_path, 'lib', 'site-packages')
        if not os.path.exists(venv_site_packages):
            # Для Windows
            venv_site_packages = os.path.join(venv_path, 'Lib', 'site-packages')
    
    # Добавляем пути к sys.path если они еще не добавлены
    paths_to_add = [p for p in site_packages if p not in sys.path]
    if venv_site_packages and venv_site_packages not in sys.path:
        paths_to_add.append(venv_site_packages)
    
    for path in paths_to_add:
        print(f"[DEBUG] Добавляем путь в sys.path: {path}")
        sys.path.append(path)
        
    # Проверяем наличие pytz в site-packages
    for path in site_packages + ([venv_site_packages] if venv_site_packages else []):
        pytz_path = os.path.join(path, 'pytz')
        if os.path.exists(pytz_path):
            print(f"[DEBUG] pytz найден по пути: {pytz_path}")
            if pytz_path not in sys.path:
                sys.path.append(pytz_path)
except Exception as e:
    print(f"[ERROR] Ошибка при поиске pytz: {e}")

import re
from typing import Optional, Tuple
import threading

# Кроссплатформенный путь к базе
import os
DB_DIR = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), '..', 'storage', 'plugins')
DB_PATH = os.path.join(DB_DIR, 'steam_rental.db')
if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR, exist_ok=True)

# Создаем функции для работы с временными зонами на уровне модуля
from datetime import datetime, timedelta

# Пытаемся импортировать всё необходимое для работы с временной зоной Москвы
MSK_TIMEZONE = None
PYTZ_AVAILABLE = False
ZONEINFO_AVAILABLE = False

try:
    import pytz
    MSK_TIMEZONE = pytz.timezone('Europe/Moscow')
    PYTZ_AVAILABLE = True
    print("[INFO] pytz успешно импортирован, будет использоваться для временных зон")
except ImportError:
    print("[INFO] pytz не найден, пробуем альтернативные методы")
    try:
        from zoneinfo import ZoneInfo
        MSK_TIMEZONE = ZoneInfo('Europe/Moscow')
        ZONEINFO_AVAILABLE = True
        print("[INFO] zoneinfo успешно импортирован, будет использоваться для временных зон")
    except ImportError:
        print("[INFO] Ни pytz, ни zoneinfo не найдены, будет использоваться ручное смещение UTC+3")

# Функция для форматирования времени в московской зоне
def format_msk_time(timestamp):
    """
    Форматирует unix timestamp в строку с московским временем
    
    Args:
        timestamp: Unix timestamp
        
    Returns:
        str: Отформатированная строка с временем в МСК
    """
    try:
        if PYTZ_AVAILABLE:
            dt = datetime.fromtimestamp(timestamp, MSK_TIMEZONE)
            return dt.strftime("%d.%m.%Y, %H:%M (MSK)")
        elif ZONEINFO_AVAILABLE:
            dt = datetime.fromtimestamp(timestamp).replace(tzinfo=MSK_TIMEZONE)
            return dt.strftime("%d.%m.%Y, %H:%M (MSK)")
        else:
            # Ручной расчет UTC+3
            dt = datetime.utcfromtimestamp(timestamp) + timedelta(hours=3)
            return dt.strftime("%d.%m.%Y, %H:%M (MSK)")
    except Exception as e:
        print(f"[ERROR] Ошибка форматирования времени: {e}")
        try:
            # Запасной вариант без временной зоны
            return datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y, %H:%M")
        except Exception as e2:
            print(f"[ERROR] Критическая ошибка форматирования времени: {e2}")
            return str(timestamp)  # Возвращаем timestamp как есть в случае полного краха

# --- Поиск свободного аккаунта по игре ---


def find_free_account(game_name: str) -> Optional[Tuple]:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "SELECT * FROM accounts WHERE game_name=? AND status='free' LIMIT 1", (game_name,))
    acc = c.fetchone()
    conn.close()
    return acc

# --- Пометить аккаунт как арендованный ---


def mark_account_rented(account_id, tg_user_id, rented_until=None, bonus_seconds=None, order_id=None):
    """Помечает аккаунт как арендованный"""
    import sqlite3
    import time
    from time import time as current_time
    
    # Максимальное количество попыток и задержка между ними
    max_attempts = 5
    retry_delay = 0.5
    
    for attempt in range(max_attempts):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=20)  # Увеличиваем timeout до 20 секунд
            c = conn.cursor()
            
            # Получаем текущее время аренды
            c.execute("SELECT rented_until FROM accounts WHERE id=?", (account_id,))
            current_rented_until = c.fetchone()
            
            if current_rented_until and current_rented_until[0]:
                current_until = float(current_rented_until[0])
                if current_until > current_time():
                    # Если есть bonus_seconds, добавляем их к текущему времени
                    if bonus_seconds:
                        new_until = current_until + bonus_seconds
                        logger.debug(f"[RENT] Добавляем {bonus_seconds}с бонусного времени к аккаунту {account_id}. Текущее рассчитанное время: {current_until}, Новое время: {new_until}")
                    else:
                        new_until = current_until
                else:
                    # Если текущее время истекло, используем новое время
                    new_until = rented_until if rented_until else (current_time() + 3600)
            else:
                # Если нет текущего времени, используем новое
                new_until = rented_until if rented_until else (current_time() + 3600)
            
            # Обновляем статус аккаунта
            c.execute("""
                UPDATE accounts 
                SET status='rented', 
                    tg_user_id=?, 
                    rented_until=?, 
                    order_id=?,
                    warned_10min=0
                WHERE id=?
            """, (tg_user_id, new_until, order_id, account_id))
            
            conn.commit()
            conn.close()
            
            # Форматируем время для лога
            from datetime import datetime
            msk_time = datetime.fromtimestamp(new_until).strftime('%d.%m.%Y, %H:%M')
            logger.debug(f"[RENT] Аккаунт {account_id} помечен как арендованный до {msk_time} (MSK) для пользователя {tg_user_id} с order_id {order_id}")
            
            return new_until
            
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_attempts - 1:
                logger.warning(f"[RENT] База данных заблокирована, попытка {attempt + 1} из {max_attempts}")
                time.sleep(retry_delay)
                continue
            else:
                logger.error(f"[RENT] Ошибка при обновлении статуса аккаунта {account_id}: {e}")
                raise
        except Exception as e:
            logger.error(f"[RENT] Неожиданная ошибка при обновлении статуса аккаунта {account_id}: {e}")
            raise
        finally:
            try:
                conn.close()
            except:
                pass

# --- Вернуть аккаунт в пул свободных ---


def mark_account_free(acc_id: int):
    """
    Помечает аккаунт как свободный и очищает все данные аренды
    
    Args:
        acc_id: ID аккаунта
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Проверяем наличие столбца order_id и bonus_given в таблице
    c.execute("PRAGMA table_info(accounts)")
    columns = [column[1] for column in c.fetchall()]
    
    if 'order_id' in columns and 'bonus_given' in columns:
        c.execute(
            "UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL, order_id=NULL, lot_id=NULL, warned_10min=0, bonus_given=0 WHERE id=?", (acc_id,))
    elif 'bonus_given' in columns:
        c.execute(
            "UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL, lot_id=NULL, warned_10min=0, bonus_given=0 WHERE id=?", (acc_id,))
    else:
        c.execute(
            "UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL, lot_id=NULL, warned_10min=0 WHERE id=?", (acc_id,))
    
    conn.commit()
    conn.close()


# --- Парсинг времени аренды из описания лота ---


def parse_rent_time(description: str) -> int | None:
    """
    Парсит время аренды из подробного описания лота (в секундах).
    Игнорирует названия игр и другие части текста.
    Поддерживает множественные части, например "1 час 10 минут".
    
    Args:
        description: Строка с описанием лота
        
    Returns:
        int: суммарное время в секундах или None, если не найдено
    """
    # Игнорируем названия игр и другие специальные слова
    ignored_words = [
        'days', 'day', 'to', 'die', 'die', 'the', 'and', 'or', 'for', 'with',
        'дней', 'день', 'дня', 'дням', 'днями', 'днях',
        'игра', 'игры', 'игрой', 'игре', 'игр', 'играми', 'играх'
    ]
    
    # Находим все вхождения числа + единиц времени
    parts = re.findall(
        r"(\d+)\s*(минут[аиы]?|минута|минуты|час(?:ов|а)?|час|сут(?:ок)?|сут|дней?|дн(?:ей)?|day(?:s)?|hour(?:s)?|minute(?:s)?)",
        description,
        re.IGNORECASE,
    )
    
    if not parts:
        return None
        
    total = 0
    for val, unit in parts:
        # Проверяем, не является ли это частью названия игры
        if any(word.lower() in unit.lower() for word in ignored_words):
            continue
            
        value = int(val)
        u = unit.lower()
        
        # Проверяем контекст - если это часть названия игры, пропускаем
        if "game" in u or "игра" in u:
            continue
            
        if "мин" in u or "min" in u:
            total += value * 60
        elif "час" in u or "hour" in u:
            total += value * 60 * 60
        elif "сут" in u or "day" in u or "дн" in u:
            total += value * 24 * 60 * 60
            
    return total if total > 0 else None

# --- Таймер для завершения аренды ---


def auto_end_rent(acc_id: int, tg_user_id: int, rent_seconds: int, notify_callback=None):
    import threading
    import logging

    logger = logging.getLogger("auto_end_rent")
    logger.info(
        f"[AUTO_END_RENT] Запущен таймер завершения аренды для аккаунта {acc_id}, пользователя {tg_user_id}, на {rent_seconds} секунд")

    def warn_before_end():
        import time
        import sqlite3
        try:
            sleep_time = rent_seconds - 600
            if sleep_time > 0:
                time.sleep(sleep_time)
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute(
                    "SELECT status, tg_user_id, rented_until, warned_10min FROM accounts WHERE id=?", (acc_id,))
                row = c.fetchone()
                if row and row[0] == 'rented' and row[1] == tg_user_id:
                    left = int(row[2] - time.time())
                    warned_10min = row[3] if len(row) > 3 else 0
                    if left <= 600 and not warned_10min:
                        try:
                            from funpay_integration import FunPayListener
                            funpay = FunPayListener()
                            msg = '🔔 До конца аренды осталось 10 минут.\n\n' \
                                  'Для продления — повторно оплатите товар на нужный срок.'
                            funpay.account.send_message(tg_user_id, msg)
                            c.execute(
                                "UPDATE accounts SET warned_10min=1 WHERE id=?", (acc_id,))
                            conn.commit()
                            logger.info(
                                f"[AUTO_END_RENT][WARN] Отправлено предупреждение о завершении аренды через 10 минут для аккаунта {acc_id}")
                        except Exception as e:
                            import traceback
                            logger.error(
                                f'[AUTO_END_RENT][ERROR] Не удалось отправить предупреждение за 10 минут: {e}')
                            traceback.print_exc()
                conn.close()
        except Exception as e:
            logger.error(
                f'[AUTO_END_RENT][ERROR] Ошибка в warn_before_end: {e}')

    threading.Thread(target=warn_before_end, daemon=True).start()
    logger.info(
        f"[AUTO_END_RENT] Запущен поток для предупреждения о завершении аренды")

    def worker():
        import time
        import os
        import sqlite3
        from db.accounts import get_account_by_id
        from utils.email_utils import fetch_steam_guard_code_from_email
        from utils.password import generate_password
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

        # Ждем указанное время аренды, но проверяем актуальное время каждую минуту
        while True:
            # Проверяем актуальное время аренды в базе
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT rented_until FROM accounts WHERE id=? AND status='rented'", (acc_id,))
            row = c.fetchone()
            conn.close()
            
            if not row or not row[0]:
                logger.info(f"[AUTO_END_RENT] Аккаунт {acc_id} больше не в аренде, отмена завершения")
                return
                
            current_time = time.time()
            rented_until = float(row[0])
            
            if rented_until <= current_time:
                break
                
            # Ждем 60 секунд перед следующей проверкой
            time.sleep(60)

        # Проверяем, что аккаунт всё еще в аренде
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "SELECT status, login, password FROM accounts WHERE id=?", (acc_id,))
        row = c.fetchone()
        conn.close()

        if not row or row[0] != 'rented':
            logger.info(
                f"[AUTO_END_RENT] Аккаунт {acc_id} больше не в аренде или не найден, отмена смены данных")
            return

        logger.info(
            f"[AUTO_END_RENT] Начинаем процесс завершения аренды аккаунта {acc_id}")

        # Получаем данные аккаунта
        acc = get_account_by_id(acc_id)
        if not acc:
            logger.error(
                f"[AUTO_END_RENT] Аккаунт {acc_id} не найден в базе данных")
            return

        login, password, email_login, email_password, imap_host = acc
        logger.info(
            f"[AUTO_END_RENT] Получены данные аккаунта {acc_id}: {login}, почта: {email_login}")

        # Подготовка директорий для сессий и скриншотов
        SESSIONS_DIR = os.path.join(os.path.dirname(
                                os.path.abspath(__file__)), '..', 'sessions')
        SCREENSHOTS_DIR = os.path.join(os.path.dirname(
            os.path.abspath(__file__)), '..', 'screenshots')
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

        session_file = os.path.join(
                                SESSIONS_DIR, f"steam_{login}.json")
        logger.info(f"[AUTO_END_RENT] Файл сессии: {session_file}")

        # Генерируем новый пароль для смены
        new_password = generate_password(length=12, special_chars=True)
        logger.info(
            f"[AUTO_END_RENT] Сгенерирован новый пароль для аккаунта {acc_id}")

        # Логика смены данных через синхронный Playwright (как в cb_change_data)
        try:
            from utils.browser_config import get_browser_config
            config = get_browser_config()
            
            with sync_playwright() as p:
                browser = p.chromium.launch(**config)
                context = None
                logged_in = False

                # Попытка использовать сохраненную сессию
                if os.path.exists(session_file):
                    logger.info(
                        f"[AUTO_END_RENT] Найден файл сессии, пробуем использовать")
                    try:
                        context = browser.new_context(
                            storage_state=session_file)
                        page = context.new_page()
                        page.goto("https://store.steampowered.com/account/")
                        page.wait_for_selector(
                            "#account_pulldown", timeout=10000)
                        logged_in = True
                        logger.info(
                            f"[AUTO_END_RENT] Успешный вход по сессии для аккаунта {login}")
                        # Сохраняем скриншот для подтверждения
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_login_session_{acc_id}.png"))
                    except Exception as e:
                        logged_in = False
                        logger.warning(
                            f"[AUTO_END_RENT] Не удалось войти по сессии: {e}")
                        if context:
                            context.close()
                            context = None

                # Если не удалось войти по сессии, выполняем обычный вход
                if not logged_in:
                    logger.info(
                        f"[AUTO_END_RENT] Выполняем обычный вход для аккаунта {login}")
                    context = browser.new_context(
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                        viewport=None,
                        locale="ru-RU",
                        java_script_enabled=True,
                        ignore_https_errors=True
                    )
                    page = context.new_page()

                    # Переходим на страницу входа
                    page.goto("https://store.steampowered.com/login/")
                    page.wait_for_load_state("networkidle")

                    # Сохраняем скриншот страницы входа
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_login_start_{acc_id}.png"))

                    # Проверяем, что мы на странице логина
                    if "login" not in page.url:
                        logger.error(
                            f"[AUTO_END_RENT] Неожиданный URL после перехода на страницу логина: {page.url}")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_login_error_{acc_id}.png"))
                        return

                    # Ждем поля ввода логина и вводим данные
                    try:
                        page.wait_for_selector(
                            'input[type="text"]', timeout=20000)
                    except PWTimeoutError:
                        logger.error(
                            "[AUTO_END_RENT] Поле логина не найдено на странице")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_login_no_fields_{acc_id}.png"))
                        return

                    # Вводим логин и пароль
                    page.fill('input[type="text"]', login)
                    page.fill('input[type="password"]', password)
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_login_filled_{acc_id}.png"))

                    # Нажимаем кнопку входа
                    page.click("button[type='submit']")

                    # Ждем либо Steam Guard, либо успешный вход, либо ошибку
                    try:
                        page.wait_for_selector(
                            "#auth_buttonset_entercode, input[maxlength='1'], #account_pulldown, .newlogindialog_FormError", timeout=25000)
                    except PWTimeoutError:
                        logger.error(
                            "[AUTO_END_RENT] Время ожидания ответа от Steam истекло")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_login_timeout_{acc_id}.png"))
                        return

                    # Проверяем необходимость ввода Steam Guard
                    need_guard = False
                    if page.query_selector("#auth_buttonset_entercode") or page.query_selector("input[maxlength='1']"):
                        need_guard = True

                    if need_guard:
                        logger.info("[AUTO_END_RENT] Требуется код Steam Guard, получаем с почты")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_steam_guard_page_{acc_id}.png"))
                        
                        # Получаем код с почты
                        if not (email_login and email_password and imap_host):
                            logger.error("[AUTO_END_RENT] Для этого аккаунта не настроена почта")
                            return
                        
                        code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, mode='change')
                        if not code:
                            logger.error("[AUTO_END_RENT] Не удалось получить код Steam Guard с почты")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_no_confirmation_code_{acc_id}.png"))
                            return
                        
                        logger.debug(f"[AUTO_END_RENT] Получен код Steam Guard: {code}")
                        
                        # Вводим код в зависимости от типа формы
                        if page.query_selector("input[maxlength='1']"):
                            inputs = page.query_selector_all("input[maxlength='1']")
                            for i, ch in enumerate(code):
                                if i < len(inputs):
                                    inputs[i].fill(ch)
                        elif page.query_selector("input[name='authcode']"):
                            page.fill("input[name='authcode']", code)
                            btn = page.query_selector("button[type='submit']")
                            if btn:
                                btn.click()
                        
                        # Ждем результата ввода кода
                        try:
                            page.wait_for_selector("#account_pulldown, .newlogindialog_FormError", timeout=15000)
                        except PWTimeoutError:
                            logger.error(
                                "[AUTO_END_RENT] Время ожидания после ввода кода истекло")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_code_timeout_{acc_id}.png"))
                            return

                        # Проверяем успешность входа
                        if page.query_selector("#account_pulldown"):
                            logged_in = True
                            logger.info("[AUTO_END_RENT] Успешный вход после ввода кода Steam Guard")
                            # Сохраняем сессию для будущего использования
                            try:
                                context.storage_state(path=session_file)
                                logger.info(f"[AUTO_END_RENT] Сессия сохранена: {session_file}")
                            except Exception as ex:
                                logger.warning(f"[AUTO_END_RENT] Не удалось сохранить сессию: {ex}")
                        elif page.query_selector(".newlogindialog_FormError"):
                            err = page.inner_text(".newlogindialog_FormError")
                            logger.error(f"[AUTO_END_RENT] Ошибка входа: {err}")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_login_error_{acc_id}.png"))
                            return
                    # Проверяем, успешен ли вход без Steam Guard
                    elif page.query_selector("#account_pulldown"):
                        logged_in = True
                        logger.info("[AUTO_END_RENT] Успешный вход без Steam Guard")
                        # Сохраняем сессию для будущего использования
                        try:
                            context.storage_state(path=session_file)
                            logger.info(f"[AUTO_END_RENT] Сессия сохранена: {session_file}")
                        except Exception as ex:
                            logger.warning(f"[AUTO_END_RENT] Не удалось сохранить сессию: {ex}")
                    elif page.query_selector(".newlogindialog_FormError"):
                        err = page.inner_text(".newlogindialog_FormError")
                        logger.error(f"[AUTO_END_RENT] Ошибка входа: {err}")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_login_error_{acc_id}.png"))
                        return
                
                # Если успешно вошли, начинаем процесс смены данных
                if logged_in:
                    logger.info("[AUTO_END_RENT] Успешный вход в аккаунт, начинаем смену данных")
                    
                    # Переходим на страницу аккаунта
                    page.goto("https://store.steampowered.com/account/")
                    page.wait_for_load_state("networkidle")
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_account_page_{acc_id}.png"))
                    
                    # Вместо поиска и клика по 'Сменить пароль', переходим напрямую по URL
                    try:
                        page.goto("https://help.steampowered.com/wizard/HelpChangePassword?redir=store/account/")
                        logger.info("[AUTO_END_RENT] Перешли на страницу смены пароля напрямую по URL")
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Не удалось перейти на страницу смены пароля: {e}")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_change_pass_fail_{acc_id}.png"))
                        return
                        
                    # Ждем загрузки страницы смены пароля
                    page.wait_for_load_state("networkidle")
                    time.sleep(2)
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_change_pass_page_{acc_id}.png"))
                    
                    # Сначала нажимаем на кнопку отправки кода подтверждения на почту
                    try:
                        # Проверяем наличие разных вариантов кнопки отправки кода
                        send_code_clicked = False
                        for selector in [
                            'a.help_wizard_button.help_wizard_arrow_right',
                            'a.help_wizard_button',
                            'a[href*="HelpWithLoginInfoEnterCode"]',
                            'a:has-text("Отправить код подтверждения")',
                            'a:has-text("Send confirmation code")',
                            'button:has-text("Отправить код")', 
                            'button:has-text("Send code")',
                            'button:has-text("Отправить код подтверждения")',
                            'button:has-text("Send confirmation code")',
                            'button:has-text("Email code")',
                            'button[type="submit"]',
                            '.ChangePasswordForm_SendEmail_1QJTx',
                            '#send_email_btn',
                            '.btn_green_steamui'
                        ]:
                            try:
                                if page.query_selector(selector):
                                    logger.info(f"[AUTO_END_RENT] Найдена кнопка отправки кода: {selector}")
                                    page.click(selector, timeout=5000)
                                    send_code_clicked = True
                                    logger.info("[AUTO_END_RENT] Кликнули по кнопке отправки кода подтверждения")
                                    
                                    # Делаем дополнительную паузу после клика
                                    time.sleep(2)
                                    break
                            except Exception as e:
                                logger.warning(f"[AUTO_END_RENT] Не удалось кликнуть по селектору {selector}: {e}")
                                continue
                        
                        if not send_code_clicked:
                            logger.warning("[AUTO_END_RENT] Не удалось найти кнопку отправки кода. Возможно, она не требуется или имеет другой селектор.")
                            # Сохраняем скриншот для анализа
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_no_send_code_btn_{acc_id}.png"))
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Ошибка при нажатии на кнопку отправки кода: {e}")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_send_code_error_{acc_id}.png"))
                    
                    # Ждем несколько секунд после нажатия кнопки отправки кода
                    logger.info("[AUTO_END_RENT] Ждем несколько секунд после отправки кода...")
                    time.sleep(5)
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_after_send_code_{acc_id}.png"))
                    
                    # Проверяем, нужно ли ввести код подтверждения с почты
                    try:
                        password_fields_found = False
                        
                        logger.info("[AUTO_END_RENT] Ищем поле для ввода кода подтверждения с почты...")
                        
                        page.wait_for_load_state("networkidle", timeout=15000)
                        
                        if page.query_selector('form#forgot_login_code_form'):
                            logger.info("[AUTO_END_RENT] Найдена форма для ввода кода подтверждения (forgot_login_code_form)")
                        elif page.query_selector('.help_page_title:has-text("Код подтверждения был отправлен")'):
                            logger.info("[AUTO_END_RENT] Найдена страница с кодом подтверждения (текст заголовка)")
                        
                        time.sleep(3)
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_code_field_search_{acc_id}.png"))
                        
                        with open(os.path.join(SCREENSHOTS_DIR, f"auth_code_page_{acc_id}.html"), "w", encoding="utf-8") as f:
                            f.write(page.content())
                            logger.info(f"[AUTO_END_RENT] Сохранили HTML страницы для диагностики")
                        
                        code_field_found = False
                        for code_field_selector in [
                            '#forgot_login_code',
                            'input[name="code"]',
                            'form#forgot_login_code_form input[type="text"]',
                            'input[type="text"][maxlength]',
                            '#authcode',
                            'input[name="authcode"]',
                            'input[placeholder*="код"]',
                            'input[placeholder*="code"]',
                            'input.auth_code'
                        ]:
                            if page.query_selector(code_field_selector):
                                logger.info(f"[AUTO_END_RENT] Найдено поле для ввода кода: {code_field_selector}")
                                code_field_found = True
                                
                                logger.info("[AUTO_END_RENT] Запрашиваем код подтверждения с почты...")
                                code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, mode='change')
                                if code:
                                    logger.info(f"[AUTO_END_RENT] Получен код подтверждения с почты: {code}")
                                    page.fill(code_field_selector, code)
                                    logger.info("[AUTO_END_RENT] Код подтверждения введен")
                                    
                                    page.screenshot(path=os.path.join(
                                        SCREENSHOTS_DIR, f"auto_end_code_entered_{acc_id}.png"))
                                    
                                    for btn_selector in [
                                        'input[type="submit"][value="Продолжить"]',
                                        'form#forgot_login_code_form input[type="submit"]',
                                        'div.account_recovery_submit input',
                                        'button:has-text("Продолжить")',
                                        'button:has-text("Continue")',
                                        'button:has-text("Submit")',
                                        'button:has-text("Подтвердить")',
                                        'button[type="submit"]',
                                        'input[type="submit"]',
                                        '.btn_green_steamui',
                                        'a.help_wizard_button'
                                    ]:
                                        try:
                                            if page.query_selector(btn_selector):
                                                logger.info(f"[AUTO_END_RENT] Нажимаем кнопку после ввода кода: {btn_selector}")
                                                page.click(btn_selector)
                                                logger.info(f"[AUTO_END_RENT] Нажата кнопка после ввода кода: {btn_selector}")
                                                break
                                        except Exception as e:
                                            logger.warning(f"[AUTO_END_RENT] Не удалось нажать кнопку {btn_selector}: {e}")
                                    
                                    logger.info("[AUTO_END_RENT] Ждем обработку кода подтверждения...")
                                    time.sleep(5)
                                    page.screenshot(path=os.path.join(
                                        SCREENSHOTS_DIR, f"auto_end_after_code_submit_{acc_id}.png"))
                                    
                                    try:
                                        page.wait_for_load_state("networkidle", timeout=15000)
                                        logger.info("[AUTO_END_RENT] Страница загружена после отправки кода подтверждения")
                                        
                                        if page.query_selector('input[type="password"]'):
                                            logger.info("[AUTO_END_RENT] Обнаружены поля пароля после ввода кода подтверждения")
                                            password_fields_found = True
                                        else:
                                            errors = page.query_selector('.help_page_title:has-text("ошибка"), .error, .ErrorText')
                                            if errors:
                                                error_text = errors.inner_text()
                                                logger.error(f"[AUTO_END_RENT] Ошибка после отправки кода: {error_text}")
                                                if "неверный" in error_text.lower() or "код" in error_text.lower():
                                                    logger.info("[AUTO_END_RENT] Пробуем запросить новый код и повторить ввод")
                                                    new_code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, mode='change', force_new=True)
                                                    if new_code and new_code != code:
                                                        logger.info(f"[AUTO_END_RENT] Получен новый код: {new_code}")
                                                        if page.query_selector(code_field_selector):
                                                            page.fill(code_field_selector, new_code)
                                                            logger.info("[AUTO_END_RENT] Введен новый код подтверждения")
                                                            for btn in page.query_selector_all('input[type="submit"], button[type="submit"]'):
                                                                try:
                                                                    btn.click()
                                                                    logger.info("[AUTO_END_RENT] Нажата кнопка подтверждения с новым кодом")
                                                                    break
                                                                except Exception as e:
                                                                    logger.warning(f"[AUTO_END_RENT] Не удалось нажать кнопку: {e}")
                                    except Exception as e:
                                        logger.warning(f"[AUTO_END_RENT] Ошибка при ожидании загрузки страницы после кода: {e}")
                                else:
                                    logger.error("[AUTO_END_RENT] Не удалось получить код подтверждения с почты")
                                    page.screenshot(path=os.path.join(SCREENSHOTS_DIR, f"auto_end_no_confirmation_code_{acc_id}.png"))
                                break
                        
                        if not code_field_found:
                            logger.info("[AUTO_END_RENT] Поле для ввода кода подтверждения не найдено. Возможно, код не требуется.")
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Ошибка при обработке кода подтверждения: {e}")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_code_error_{acc_id}.png"))
                    
                    logger.info("[AUTO_END_RENT] Ожидаем появления полей для ввода пароля...")
                    
                    if password_fields_found:
                        logger.info("[AUTO_END_RENT] Поля пароля уже обнаружены ранее, пропускаем ожидание")
                    else:
                        try:
                            page.wait_for_selector('input[type="password"]', timeout=30000)
                            logger.info("[AUTO_END_RENT] Найдены поля для ввода пароля")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_password_fields_found_{acc_id}.png"))
                        except PWTimeoutError as e:
                            logger.error(f"[AUTO_END_RENT] Не удалось найти поля для пароля: {e}")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_no_pass_fields_{acc_id}.png"))
                            
                            try:
                                logger.info("[AUTO_END_RENT] Пробуем обновить страницу и найти поля пароля снова...")
                                page.reload()
                                page.wait_for_load_state("networkidle")
                                time.sleep(3)
                                
                                if page.query_selector('input[type="password"]'):
                                    logger.info("[AUTO_END_RENT] Найдены поля для пароля после обновления страницы")
                                    page.screenshot(path=os.path.join(
                                        SCREENSHOTS_DIR, f"auto_end_pass_fields_after_reload_{acc_id}.png"))
                                else:
                                    logger.error("[AUTO_END_RENT] Поля для пароля не найдены даже после обновления страницы")
                                    return
                            except Exception as reload_err:
                                logger.error(f"[AUTO_END_RENT] Ошибка при попытке обновить страницу: {reload_err}")
                                return
                    
                    password_fields = page.query_selector_all('input[type="password"]')
                    if len(password_fields) >= 2:
                        password_fields[0].fill(new_password)
                        password_fields[1].fill(new_password)
                        logger.info("[AUTO_END_RENT] Ввели новый пароль в оба поля (по порядку)")
                    else:
                        filled = False
                        for sel in [
                            'input[placeholder*="Change my password"]',
                            'input[placeholder*="Re-enter"]',
                            'input[name*="new_password"]',
                            'input[name*="reenter"]'
                        ]:
                            try:
                                page.fill(sel, new_password)
                                logger.info(f"[AUTO_END_RENT] Ввели новый пароль в поле {sel}")
                                filled = True
                            except Exception:
                                pass
                                
                        if not filled:
                            logger.error("[AUTO_END_RENT] Не удалось найти оба поля для ввода нового пароля")
                            page.screenshot(path=os.path.join(
                                SCREENSHOTS_DIR, f"auto_end_fill_fail_{acc_id}.png"))
                            return
                    
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_filled_{acc_id}.png"))
                    
                    clicked = False
                    for sel in [
                        'button:has-text("Сменить пароль"):not([disabled])',
                        'button:has-text("Сменить пароль")',
                        '#change_password_button',
                        '.change_password_button',
                        'button:has-text("Change Password"):not([disabled])',
                        'button:has-text("Change Password")',
                        'button[type="submit"]',
                        'input[type="submit"]'
                    ]:
                        try:
                            page.click(sel, timeout=3000)
                            logger.info(f"[AUTO_END_RENT] Нажали на кнопку смены пароля через селектор: {sel}")
                            clicked = True
                            break
                        except Exception:
                            continue
                            
                    if not clicked:
                        logger.error("[AUTO_END_RENT] Не удалось нажать на кнопку смены пароля ни одним из селекторов")
                        page.screenshot(path=os.path.join(
                            SCREENSHOTS_DIR, f"auto_end_button_fail_{acc_id}.png"))
                        return
                    
                    logger.info("[AUTO_END_RENT] Нажали кнопку смены пароля. Ожидаем результат...")
                    time.sleep(4)
                    
                    page.screenshot(path=os.path.join(
                        SCREENSHOTS_DIR, f"auto_end_result_{acc_id}.png"))
                    
                    content = page.content()
                    success = False
                    if "success" in content.lower() or "пароль успешно" in content.lower():
                        logger.info(f"[AUTO_END_RENT] Пароль успешно изменён! Новый пароль: {new_password}")
                        
                        from db.accounts import update_account_password
                        update_account_password(acc_id, new_password)
                        logger.info(f"[AUTO_END_RENT] Данные аккаунта обновлены в БД: {acc_id}")
                        success = True
                    else:
                        logger.warning("[AUTO_END_RENT] Не удалось однозначно определить успешную смену пароля")
                
                if context:
                    context.close()
                if browser:
                    browser.close()
                
                if success or rent_seconds <= 60:
                    logger.info(f"[AUTO_END_RENT] Сбрасываем статус аккаунта {acc_id} на 'free'")
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    
                    # Получаем order_id аккаунта перед сбросом статуса
                    order_id = None
                    try:
                        # Проверяем наличие столбца order_id в таблице accounts
                        c.execute("PRAGMA table_info(accounts)")
                        columns = [column[1] for column in c.fetchall()]
                        
                        if 'order_id' in columns:
                            c.execute("SELECT order_id FROM accounts WHERE id=?", (acc_id,))
                            row = c.fetchone()
                            if row and row[0]:
                                order_id = row[0]
                                logger.info(f"[AUTO_END_RENT] Получен order_id: {order_id} для аккаунта {acc_id}")
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Ошибка при получении order_id: {e}")
                    
                    # Сбрасываем статус аккаунта
                    c.execute("UPDATE accounts SET status='free', rented_until=NULL, tg_user_id=NULL, order_id=NULL, lot_id=NULL, warned_10min=0, bonus_given=0 WHERE id=?", (acc_id,))
                    conn.commit()
                    
                    # Проверяем, что все поля корректно сброшены
                    try:
                        c.execute("""
                            SELECT status, rented_until, tg_user_id, order_id, lot_id, warned_10min, bonus_given 
                            FROM accounts WHERE id=?
                        """, (acc_id,))
                        row = c.fetchone()
                        if row:
                            status, rented_until, tg_user_id, order_id, lot_id, warned_10min, bonus_given = row
                            if (status != 'free' or rented_until is not None or tg_user_id is not None or 
                                order_id is not None or lot_id is not None or warned_10min != 0 or bonus_given != 0):
                                logger.error(f"[AUTO_END_RENT] Ошибка сброса полей для аккаунта {acc_id}:")
                                logger.error(f"status={status}, rented_until={rented_until}, tg_user_id={tg_user_id}")
                                logger.error(f"order_id={order_id}, lot_id={lot_id}, warned_10min={warned_10min}, bonus_given={bonus_given}")
                                # Пробуем сбросить поля повторно
                                c.execute("""
                                    UPDATE accounts 
                                    SET status='free', rented_until=NULL, tg_user_id=NULL, 
                                        order_id=NULL, lot_id=NULL, warned_10min=0, bonus_given=0 
                                    WHERE id=?
                                """, (acc_id,))
                                conn.commit()
                                logger.info(f"[AUTO_END_RENT] Повторная попытка сброса полей для аккаунта {acc_id}")
                            else:
                                logger.info(f"[AUTO_END_RENT] Все поля успешно сброшены для аккаунта {acc_id}")
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Ошибка при проверке сброса полей для аккаунта {acc_id}: {e}")
                    
                    conn.close()
                    
                    try:
                        from funpay_integration import FunPayListener
                        funpay = FunPayListener()
                        # Создаем правильный объект заказа с order_id
                        order_data = {
                            'chat_id': tg_user_id,
                            'order_id': order_id  # Используем только order_id из БД
                        }
                        # Отправляем сообщение о завершении заказа
                        send_order_completed_message(order_data, 
                            lambda chat_id, text: funpay.funpay_send_message_wrapper(chat_id, text))
                        logger.info(f"[AUTO_END_RENT] Отправка уведомления через notify_callback клиенту {tg_user_id} об окончании аренды")
                    except Exception as e:
                        logger.error(f"[AUTO_END_RENT] Не удалось инициализировать FunPayListener: {e}")
                    
                    if notify_callback:
                        try:
                            notify_callback(acc_id, tg_user_id)
                            logger.info(f"[AUTO_END_RENT] Вызван notify_callback для аккаунта {acc_id}")
                        except Exception as e:
                            logger.error(f"[AUTO_END_RENT] Ошибка в notify_callback: {e}")
                else:
                    logger.error(f"[AUTO_END_RENT] Не удалось изменить пароль для аккаунта {acc_id}, статус не сброшен")
                
        except Exception as e:
            logger.error(f"[AUTO_END_RENT] Критическая ошибка при завершении аренды: {e}", exc_info=True)
    
    threading.Thread(target=worker, daemon=True).start()
    logger.info(f"[AUTO_END_RENT] Запущен основной поток для завершения аренды аккаунта {acc_id}")
    return True

# --- Пример функции отправки аккаунта покупателю (заготовка) ---
def send_account_to_buyer(order, acc, send_func):
    """
    order: dict или объект заказа (должен содержать chat_id, buyer, description и т.д.)
    acc: tuple с данными аккаунта
    send_func: функция отправки сообщения (например, bot.send_message)
    """
    login, password, game_name = acc[1], acc[2], acc[3]
    msg = f"🎮 Ваш арендованный Steam-аккаунт:\n\n"
    msg += f"💼 Логин: {login}\n"
    msg += f"🔑 Пароль: {password}\n\n"
    msg += f"Для входа в аккаунт используйте клиент Steam."
    send_func(order['chat_id'], msg)

# Функция для отправки кода Steam Guard с указанием срока окончания аренды
def send_steam_guard_code(chat_id, code, until_timestamp, send_func):
    """
    Отправляет код Steam Guard покупателю с указанием времени окончания аренды
    
    Args:
        chat_id: ID чата для отправки сообщения
        code: Код Steam Guard
        until_timestamp: Unix timestamp окончания аренды
        send_func: Функция для отправки сообщения (должна принимать chat_id и text)
    """
    try:
        formatted_time = format_msk_time(until_timestamp)
        
        msg = f"🔐 Код Steam Guard: {code}\n\n"
        msg += f"⏰ Аренда действительна до: {formatted_time}\n\n"
        msg += f"Введите код в клиенте Steam при входе.\n"
        msg += f"Код действует ограниченное время.\n\n"
        msg += f"✨ Приятной игры!"
        
        send_func(chat_id, msg)
    except Exception as e:
        print(f"[ERROR] Ошибка отправки кода Steam Guard: {e}")
        try:
            # bez formata
            msg = f"🔐 Код Steam Guard: {code}\n\n"
            msg += f"⏰ Аренда действительна до: {until_timestamp}\n\n"
            msg += f"Введите код в клиенте Steam при входе.\n"
            msg += f"Код действует ограниченное время.\n\n"
            msg += f"✨ Приятной игры!"
            send_func(chat_id, msg)
        except Exception as e2:
            print(f"[ERROR] Критическая ошибка отправки кода Steam Guard: {e2}")
            # last
            try:
                send_func(chat_id, f"Код Steam Guard: {code}")
            except Exception as e3:
                print(f"[ERROR] Complete failure sending Steam Guard code: {e3}")

# --- Совместимость с логикой test_rental.py ---
def get_account_for_order(order_id, game_name):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT account_id FROM issued_accounts WHERE order_id = ?", (order_id,))
    row = c.fetchone()
    if row:
        c.execute("SELECT * FROM accounts WHERE id = ?", (row[0],))
        acc = c.fetchone()
        conn.close()
        return acc
    c.execute("SELECT * FROM accounts WHERE game_name = ? AND status = 'free'", (game_name,))
    acc = c.fetchone()
    conn.close()
    return acc

def mark_account_issued(order_id, account_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO issued_accounts (order_id, account_id, message_sent) VALUES (?, ?, 0)", (order_id, account_id))
    conn.commit()
    conn.close()

def set_message_sent(order_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("UPDATE issued_accounts SET message_sent = 1 WHERE order_id = ?", (order_id,))
    conn.commit()
    conn.close()

def was_message_sent(order_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT message_sent FROM issued_accounts WHERE order_id = ?", (order_id,))
    row = c.fetchone()
    conn.close()
    return row and row[0] == 1

def set_account_rented(id, until, tg_user_id, lot_id, order_id=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Проверяем наличие столбца order_id
    c.execute("PRAGMA table_info(accounts)")
    columns = [column[1] for column in c.fetchall()]
    
    if 'order_id' in columns:
        c.execute("UPDATE accounts SET rented_until = ?, status = 'rented', tg_user_id = ?, lot_id = ?, order_id = ? WHERE id = ?",
                  (until, tg_user_id, lot_id, order_id, id))
    else:
        c.execute("UPDATE accounts SET rented_until = ?, status = 'rented', tg_user_id = ?, lot_id = ? WHERE id = ?",
                  (until, tg_user_id, lot_id, id))
    
    conn.commit()
    conn.close()

# Функция для отправки сообщения о завершении заказа
def send_order_completed_message(order, send_func):
    try:
        # Получаем order_id и chat_id из объекта заказа
        order_id = getattr(order, 'order_id', None) or order.get('order_id', None)
        chat_id = getattr(order, 'chat_id', None) or order.get('chat_id')
        
        if not chat_id:
            print("[ERROR] Не указан chat_id для отправки сообщения о выполнении заказа")
            return
        
        # Если order_id не передан или равен UNKNOWN, пробуем получить из базы данных
        if not order_id or order_id == 'UNKNOWN':
            try:
                import sqlite3
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                # Проверяем наличие столбца order_id
                c.execute("PRAGMA table_info(accounts)")
                columns = [column[1] for column in c.fetchall()]
                
                if 'order_id' in columns:
                    # Ищем order_id по chat_id (tg_user_id)
                    c.execute("SELECT order_id FROM accounts WHERE tg_user_id=? AND status='rented'", (chat_id,))
                    row = c.fetchone()
                    if row and row[0] and not (row[0].startswith('TG-') or row[0].startswith('TEST-')):
                        order_id = row[0]
                        print(f"[FunPay][ORDER] Найден ID заказа в БД: {order_id}")
                conn.close()
            except Exception as e:
                print(f"[ERROR] Не удалось получить ID заказа из базы данных: {e}")
        
        # Проверяем, что order_id не пустой и не UNKNOWN
        if not order_id or order_id == 'UNKNOWN':
            print("[WARNING] Не удалось определить ID заказа для сообщения о завершении")
            return  # Прерываем выполнение, если не удалось получить order_id
        
        # Формируем сообщение для FunPay-ордеров (без форматирования)
        message = (
            f"🔔 Заказ #{order_id} успешно выполнен!\n\n"
            f"Если все прошло успешно, пожалуйста, нажмите кнопку \"Подтвердить выполнение заказа\" ✅ на странице https://funpay.com/orders/{order_id}/. Будем рады видеть вас снова! ❤️\n\n"
            f"Если у вас возникли какие-либо трудности с заказом, сообщите нам в чат, и мы постараемся помочь вам как можно скорее.\n\n"
            f"Мы будем очень признательны, если вы оставите нам отзыв 🌟🌟🌟🌟🌟!"
        )
        send_func(chat_id, message)
    except Exception as e:
        print(f"[ERROR] Не удалось отправить сообщение о выполнении заказа: {e}")
