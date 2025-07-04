from imap_tools import MailBox, AND
import re
from datetime import datetime, timedelta
import time

def fetch_steam_guard_code_from_email(email_login, email_password, imap_host, timeout=600, logger=None, mode='login', force_new=True, start_time=None):
    """
    mode: 'login' — для входа (обычный Steam Guard), 'change' — для смены данных (change credentials).
    force_new: если True, игнорирует ранее проверенные письма и ищет только новые.
    start_time: время начала поиска (если None, используется текущее время минус 15 минут)
    timeout: максимальное время ожидания кода в секундах (по умолчанию 10 минут)
    """
    if logger:
        logger.info(f"[EMAIL] Начинаем поиск кода Steam Guard")
        logger.info(f"[EMAIL] Режим: {mode}, Email: {email_login[:3]}***@{email_login.split('@')[1] if '@' in email_login else 'unknown'}")
        logger.info(f"[EMAIL] IMAP хост: {imap_host}, Таймаут: {timeout} сек")
    
    if not (email_login and email_password and imap_host):
        if logger:
            logger.warning(f"[EMAIL] Нет почтовых данных: email={email_login}, imap_host={imap_host}")
        return None
    
    # Защита от некорректного timeout
    if timeout is None or not isinstance(timeout, (int, float)) or timeout <= 0:
        if logger:
            logger.warning(f"[EMAIL] Некорректный timeout: {timeout}, используется значение по умолчанию 600")
        timeout = 600
    
    # Сохраняем время начала поиска сразу
    search_start_datetime = datetime.utcnow()
    
    code_regex = re.compile(r"\b([A-Z0-9]{5,7})\b")
    
    code_after_label_regex = re.compile(r"(?:ваш\s+код|code|код|код\s*:|code\s*:)\s*[\"']?([A-Z0-9]{5,7})[\"']?", re.IGNORECASE)
    
    steam_guard_code_regex = re.compile(r"(?:steam\s*guard|подтверждения)(?:.*?)([A-Z0-9]{5,7})", re.IGNORECASE | re.DOTALL)
    
    account_confirm_regex = re.compile(r"(?:код\s+подтверждения\s+аккаунта|account\s+confirmation\s+code)(?:\s*:?\s*)([A-Z0-9]{5,7})", re.IGNORECASE | re.DOTALL)
    
    isolated_code_regex = re.compile(r"<[^>]*>\s*([A-Z0-9]{5,7})\s*</[^>]*>", re.IGNORECASE)
    
    quoted_code_regex = re.compile(r"[\"'«»]([A-Z0-9]{5,7})[\"'«»]", re.IGNORECASE)
    
    invalid_words = [
        "STEAM", "SCREEN", "HTTPS", "GUARD", "VALVE", "HELP", "LOGIN", "EMAIL", "ПОСМОТР", 
        "DOCTYPE", "HTML", "HEAD", "BODY", "DIV", "SPAN", "SCRIPT", "CLASS",
        "WIDTH", "HEIGHT", "STYLE", "COLOR", "TABLE", "TITLE", "HTTP", "META",
        "CONTENT", "FORM", "INPUT", "BUTTON", "IMAGE", "FRAME", "TYPE", "VIEW",
        "https"
    ]
    
    def is_valid_code(code, strict=False):
        if not code:
            return False
            
        if len(code) < 5 or len(code) > 7:
            if logger and strict:
                logger.warning(f"[EMAIL] Неверная длина кода: {code} (длина {len(code)})")
            return False
            
        if not code.isalnum():
            if logger and strict:
                logger.warning(f"[EMAIL] Код содержит недопустимые символы: {code}")
            return False
            
        if code.upper() in invalid_words:
            if logger:
                logger.warning(f"[EMAIL] Обнаружено недопустимое слово вместо кода: {code}")
            return False
        
        if strict and mode == 'login':
            has_letters = any(c.isalpha() for c in code)
            has_digits = any(c.isdigit() for c in code)
            
            if not (has_letters and has_digits) and len(code) != 5:
                if logger:
                    logger.warning(f"[EMAIL] Код не соответствует формату Steam Guard (должен содержать буквы и цифры): {code}")
                return False
                
            for i in range(len(code) - 2):
                if code[i] == code[i+1] == code[i+2]:
                    if logger:
                        logger.warning(f"[EMAIL] Код содержит повторяющиеся символы: {code}")
                    return False
                    
            digit_count = sum(1 for c in code if c.isdigit())
            if digit_count == len(code) and len(code) > 5:
                if logger:
                    logger.warning(f"[EMAIL] Код содержит только цифры: {code}")
                return False
                
        return True
    
    start = time.time()
    
    if force_new:
        # Если force_new=True, ищем письма точно с момента запуска функции (плюс небольшой буфер)
        min_date_obj = (search_start_datetime - timedelta(seconds=10)).date()
    else:
        # Иначе используем логику на основе start_time или стандартный интервал
        if start_time is not None:
            if isinstance(start_time, (int, float)):
                min_date = datetime.fromtimestamp(start_time - 60)
            else:
                min_date = start_time - timedelta(seconds=60)
        else:
            min_date = datetime.now() - timedelta(minutes=15)
        
        min_date_obj = min_date.date()
    
    checked_uids = set()
    
    total_checked = 0
    
    if logger:
        logger.info(f"[EMAIL] Начинаем поиск кода для {mode}. Таймаут: {timeout}с, минимальная дата: {min_date_obj}")
    
    while time.time() - start < timeout:
        try:
            elapsed = time.time() - start
            if logger:
                logger.info(f"[EMAIL] ПОПЫТКА ПОДКЛЮЧЕНИЯ к почте (прошло {elapsed:.1f}с из {timeout}с)")
            
            with MailBox(imap_host).login(email_login, email_password) as mailbox:
                if logger:
                    logger.info(f"[EMAIL] ✅ УСПЕШНОЕ ПОДКЛЮЧЕНИЕ к почтовому серверу")
                if mode == 'login':
                    login_subjects = [
                        "Steam", 
                        "new steam login", 
                        "steam guard", 
                        "вход в steam"
                    ]
                    
                    found_code = None
                    
                    if logger:
                        logger.info(f"[EMAIL] РЕЖИМ LOGIN - ищем новые письма от noreply@steampowered.com с {min_date_obj}")
                        
                    msgs = sorted(
                        mailbox.fetch(AND(seen=False, date_gte=min_date_obj, from_='noreply@steampowered.com'), reverse=True, limit=10),
                        key=lambda m: m.date if hasattr(m, 'date') and m.date else datetime.now(),
                        reverse=True
                    )
                    
                    if logger:
                        logger.info(f"[EMAIL] ✅ НАЙДЕНО {len(msgs)} писем для проверки")
                    
                    for msg in msgs:
                        if msg.uid in checked_uids:
                            continue
                            
                        checked_uids.add(msg.uid)
                        total_checked += 1
                            
                        if logger:
                            logger.info(f"[EMAIL] Проверяем письмо: {msg.subject} (UID: {msg.uid})")
                                
                        try:
                            body = (msg.text or '') + '\n' + (msg.html or '')
                            
                            text_content = msg.text or ''
                            
                            steam_guard_pattern = re.search(r"(?:вам\s+понадобится\s+код|код\s+steam\s+guard|you\s+need\s+a\s+code|your\s+steam\s+code).*?([A-Z0-9]{5})", text_content, re.IGNORECASE | re.DOTALL)
                            
                            if steam_guard_pattern:
                                found_code = steam_guard_pattern.group(1)
                                if is_valid_code(found_code, strict=True):
                                    mailbox.flag(msg.uid, 'SEEN', True)
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код для входа (стандартный шаблон): {found_code}")
                                    return found_code
                            
                            lines = text_content.split('\n')
                            for line in lines:
                                line = line.strip()
                                if 5 <= len(line) <= 7 and line.isalnum() and line.isupper():
                                    if is_valid_code(line, strict=True):
                                        mailbox.flag(msg.uid, 'SEEN', True)
                                        if logger:
                                            logger.info(f"[EMAIL] Найден код для входа (отдельная строка): {line}")
                                        return line
                            
                            for regex_name, regex in [
                                ("код после метки", code_after_label_regex),
                                ("код в контексте Steam Guard", steam_guard_code_regex),
                                ("код подтверждения аккаунта", account_confirm_regex),
                                ("код в кавычках", quoted_code_regex),
                                ("общий шаблон", code_regex)
                            ]:
                                try:
                                    m = regex.search(body)
                                    if m:
                                        found_code = m.group(1)
                                        
                                        if is_valid_code(found_code, strict=True):
                                            mailbox.flag(msg.uid, 'SEEN', True)
                                            if logger:
                                                logger.info(f"[EMAIL] Найден код для входа ({regex_name}): {found_code}")
                                            return found_code
                                        elif logger:
                                            logger.warning(f"[EMAIL] Обнаружен недействительный код ({regex_name}): {found_code}")
                                except Exception as e:
                                    if logger:
                                        logger.error(f"[EMAIL] Ошибка при поиске кода ({regex_name}): {e}")
                        except Exception as e:
                            if logger:
                                logger.error(f"[EMAIL] Ошибка при обработке письма: {e}")
                            continue
                    
                elif mode == 'change':
                    if logger:
                        logger.info(f"[EMAIL] РЕЖИМ CHANGE - поиск писем для смены данных с {min_date_obj}")
                    
                    msgs = sorted(
                        mailbox.fetch(AND(date_gte=min_date_obj), reverse=True, limit=30),
                        key=lambda m: m.date if hasattr(m, 'date') and m.date else datetime.now(),
                        reverse=True
                    )
                    
                    if logger:
                        logger.info(f"[EMAIL] ✅ НАЙДЕНО {len(msgs)} писем для проверки в режиме change")
                    found_code = None
                    for msg in msgs:
                        if msg.uid in checked_uids:
                            continue
                        subj = (msg.subject or '').lower()
                        from_ = (msg.from_ or '').lower()
                        body = (msg.text or '') + '\n' + (msg.html or '')
                        
                        if (
                            ('noreply@steampowered.com' in from_)
                            and (
                                "сменить пароль" in subj or
                                "сменить пароль" in body or
                                "смена пароля" in subj or
                                "смена пароля" in body or
                                "смены данных" in subj or
                                "смены данных" in body or
                                "change your steam login credentials" in body or
                                "verification code" in subj or
                                "код подтверждения" in subj or
                                "код подтверждения" in body or
                                "код был выслан" in body.lower() or
                                "код был отправлен" in body.lower() or
                                "подтверждение аккаунта" in body.lower() or
                                "code was sent" in body.lower() or
                                "help.steampowered.com" in body.lower()
                            )
                        ):
                            checked_uids.add(msg.uid)
                            if logger:
                                logger.info(f"[EMAIL] Проверяем письмо для смены данных: {subj}")
                            
                            found_code = None
                            
                            m = code_after_label_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код после метки: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый код после метки: {found_code}")
                                
                            m = steam_guard_code_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код в контексте Steam Guard: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый код в контексте Steam Guard: {found_code}")
                                
                            m = account_confirm_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код подтверждения аккаунта: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый код подтверждения аккаунта: {found_code}")
                                
                            m = isolated_code_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден изолированный код: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый изолированный код: {found_code}")
                            
                            m = quoted_code_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код в кавычках: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый код в кавычках: {found_code}")
                                
                            m = code_regex.search(body)
                            if m:
                                found_code = m.group(1)
                                if is_valid_code(found_code):
                                    if logger:
                                        logger.info(f"[EMAIL] Найден код по общему шаблону: {found_code}")
                                    break
                                elif logger:
                                    logger.warning(f"[EMAIL] Найден недопустимый код по общему шаблону: {found_code}")
                            else:
                                if logger:
                                    logger.warning(f"[EMAIL] В письме не найден код: {subj[:50]}")
                    if mode == 'change' and found_code:
                        if is_valid_code(found_code):
                            if logger:
                                logger.info(f"[EMAIL] Возвращаем найденный код для смены данных: {found_code}")
                            return found_code
                        elif logger:
                            logger.warning(f"[EMAIL] Найденный код недопустим: {found_code}")
                    if mode == 'change' and not found_code:
                        for msg in msgs:
                            if msg.uid in checked_uids:
                                continue
                            subj = (msg.subject or '').lower()
                            from_ = (msg.from_ or '').lower()
                            body = (msg.text or '') + '\n' + (msg.html or '')
                            if 'noreply@steampowered.com' in from_:
                                try:
                                    with open('email_fallback.log', 'a', encoding='utf-8') as flog:
                                        flog.write(f"FALLBACK UID: {msg.uid} FROM: {from_} SUBJECT: {subj}\nBODY: {body[:200]}\n\n")
                                    if logger:
                                        logger.info(f"[EMAIL] Fallback поиск кода в письме: {subj[:50]}")
                                except Exception:
                                    pass

                                m = code_after_label_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден код после метки: {found_code}")
                                        return found_code

                                m = steam_guard_code_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден код в контексте Steam Guard: {found_code}")
                                        return found_code

                                m = account_confirm_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден код подтверждения аккаунта: {found_code}")
                                        return found_code

                                m = isolated_code_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден изолированный код: {found_code}")
                                        return found_code

                                m = quoted_code_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден код в кавычках: {found_code}")
                                        return found_code

                                m = code_regex.search(body)
                                if m:
                                    found_code = m.group(1)
                                    if is_valid_code(found_code):
                                        if logger:
                                            logger.info(f"[EMAIL] Fallback: найден код по общему шаблону: {found_code}")
                                        return found_code
                                    elif logger:
                                        logger.warning(f"[EMAIL] Fallback: найден недопустимый код по общему шаблону: {found_code}")
                            checked_uids.add(msg.uid)
        except Exception as e:
            if logger:
                logger.error(f"[EMAIL] ❌ ОШИБКА подключения к почте: {e}")
                logger.error(f"[EMAIL] Тип ошибки: {type(e).__name__}")
            
            import socket
            if isinstance(e, socket.gaierror):
                if logger:
                    logger.error(f"[EMAIL] ❌ Некорректный IMAP хост: {imap_host}")
                break
            elif "authentication" in str(e).lower() or "login" in str(e).lower():
                if logger:
                    logger.error(f"[EMAIL] ❌ Ошибка авторизации - неверные email/пароль")
                break
            elif "timeout" in str(e).lower():
                if logger:
                    logger.error(f"[EMAIL] ❌ Таймаут подключения к почте")
            elif "connection" in str(e).lower():
                if logger:
                    logger.error(f"[EMAIL] ❌ Ошибка соединения с почтовым сервером")
            else:
                if logger:
                    logger.error(f"[EMAIL] ❌ Общая ошибка при поиске кода: {e}")
                    
            # Пауза перед повторной попыткой
            if logger:
                logger.info(f"[EMAIL] Ждем 5 секунд перед повторной попыткой...")
            time.sleep(5)
        
        elapsed = time.time() - start
        if elapsed < 60:
            sleep_time = 3
        elif elapsed < 180:
            sleep_time = 5
        else:
            sleep_time = 10
            
        if logger and total_checked > 0:
            remaining = timeout - elapsed
            logger.info(f"[EMAIL] Пока не нашли код. Прошло {int(elapsed)}с, осталось {int(remaining)}с. Проверено {total_checked} писем.")
            
        time.sleep(sleep_time)
    
    if logger:
        logger.warning(f"[EMAIL] Не удалось найти код за отведенное время ({timeout}с). Проверено {total_checked} писем.")
    
    return None
