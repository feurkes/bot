import os
import json
import threading
import re
import sys
from threading import Thread
from time import time, sleep
from steam.steam_account_rental_utils import find_free_account, mark_account_rented, mark_account_free, send_account_to_buyer, auto_end_rent
from dotenv import load_dotenv

import traceback

# Загружаем переменные окружения из файла .env
load_dotenv()

# Исправление проблемы с кодировкой символов на Windows и принудительная отправка вывода
import sys
import io
try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    # Отключаем буферизацию вывода, чтобы сообщения выводились немедленно
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except:
    pass  # Игнорируем ошибки, если переопределение не удалось

# Функция для вывода с принудительной очисткой буфера
def print_flush(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()  # Принудительно сбрасываем буфер

# Правильный импорт логгера
import logging
logger = logging.getLogger("funpay_integration")

try:
    from FunPayAPI.account import Account
    from FunPayAPI.updater.runner import Runner
    from FunPayAPI.updater.events import NewOrderEvent, NewMessageEvent
    from FunPayAPI.common.utils import RegularExpressions
except Exception as e:
    print_flush("[DIAGNOSE] Ошибка при импорте FunPayAPI:")
    traceback.print_exc(file=sys.stdout)
    sys.stdout.flush()
    Account = None
    Runner = None
    NewOrderEvent = None
    NewMessageEvent = None
    RegularExpressions = None

# Сохраняем поддержку старого пути к настройкам для обратной совместимости
SETTINGS_PATH = os.path.join(os.path.dirname(__file__), 'funpay_rent_settings.json')

from game_name_mapper import mapper

# Время бонуса при получении отзыва в секундах
REVIEW_BONUS_TIME = 30 * 60  # 30 минут

class FunPayListener:
    def __init__(self):
        # Получаем GOLDEN_KEY из переменных окружения
        self.golden_key = os.getenv("GOLDEN_KEY")
        
        # Если ключ не найден в .env, пробуем загрузить его из файла настроек (обратная совместимость)
        if not self.golden_key and os.path.exists(SETTINGS_PATH):
            try:
                with open(SETTINGS_PATH, encoding='utf-8') as f:
                    config = json.load(f)
                # Получаем golden_key и user_agent из конфига (кроссплатформенно)
                funpay_cfg = config.get('FunPay', {}) if isinstance(config, dict) else {}
                self.golden_key = funpay_cfg.get('golden_key') or config.get('golden_key')
                self.user_agent = funpay_cfg.get('user_agent') or config.get('user_agent')
            except Exception as e:
                logger.error(f"Ошибка при загрузке настроек из файла: {e}")
                self.golden_key = None
                self.user_agent = None
        else:
            # По умолчанию используем стандартный user_agent
            self.user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            
        if not self.golden_key:
            raise RuntimeError('Golden key не найден ни в .env, ни в файле настроек. Проверьте наличие GOLDEN_KEY в файле .env')
            
        if Account is None:
            raise ImportError('FunPayAPI не установлен!')
            
        # Передаём user_agent, если поддерживается
        try:
            self.account = Account(self.golden_key, user_agent=self.user_agent) if self.user_agent else Account(self.golden_key)
        except TypeError:
            self.account = Account(self.golden_key)
        self.account.get()  # Авторизация и загрузка данных аккаунта
        self.updater = Runner(self.account)

    def normalize_game_name(self, game_name):
        return mapper.normalize(game_name)

    def start(self):
        from FunPayAPI.updater.events import OrderStatusChangedEvent, NewMessageEvent
        import logging
        def listen():
            print_flush('[FunPay] Запуск слушателя событий...')
            while True:
                try:
                    for event in self.updater.listen():
                        if isinstance(event, NewOrderEvent):
                            print_flush(f'[FunPay][EVENT] Новый заказ: {event.order.id}')
                            self.handle_new_order(event)
                        elif isinstance(event, OrderStatusChangedEvent):
                            print_flush(f'[FunPay][EVENT] Изменение статуса заказа: {event.order.id}')
                            self.handle_order_status_changed(event)
                        elif isinstance(event, NewMessageEvent):
                            # Выводим сообщение с принудительной очисткой буфера
                            print_flush(f'[FunPay][EVENT] Новое сообщение: {getattr(event.message, "text", None)}')
                            
                            # Проверяем, является ли сообщение отзывом
                            is_review = self.handle_review_message(event)
                            
                            # Всегда обрабатываем сообщение даже если это отзыв
                            # Это позволит боту реагировать на все сообщения
                            self.handle_new_message(event)
                            
                            # Дополнительно сбрасываем буфер после обработки сообщения
                            sys.stdout.flush()
                except Exception as e:
                    print_flush(f'[FunPay][ERROR] Ошибка в слушателе событий: {e}')
                    import traceback
                    traceback.print_exc(file=sys.stdout)
                    sys.stdout.flush()
                    print_flush('[FunPay] Перезапуск слушателя через 5 секунд...')
                    import time
                    time.sleep(5)
        
        # Запускаем поток слушателя с повышенным приоритетом
        listener_thread = Thread(target=listen, daemon=True)
        listener_thread.start()
        print_flush('[FunPay] Слушатель событий запущен.')

    def handle_new_order(self, event):
        try:
            order = event.order
            # Просто логируем заказ, не выдаём аккаунт
            desc = order.description
            print_flush('[FunPay][НОВЫЙ ЗАКАЗ]')
            print_flush(f'  Покупатель: {getattr(order, "buyer_username", "—")} (ID: {getattr(order, "buyer_id", "—")})')
            print_flush(f'  Описание заказа: {desc}')
        except Exception as e:
            print_flush(f'[FunPay] Ошибка при обработке нового заказа: {e}')
            import traceback
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()

    def handle_order_status_changed(self, event):
        # Обработка изменения статуса заказа
        order = event.order
        if not order:
            return False
            
        order_id = order.id
        status = order.status
            
        # Проверка на наличие ID заказа и его статуса
        if not order_id or not status:
            return False
            
        # Для завершенных заказов
        if status == 'completed':
            chat_id = getattr(order, 'chat_id', None)
            if chat_id:
                try:
                    from steam.steam_account_rental_utils import send_order_completed_message
                    # Извлекаем только основную часть order_id, если есть суффикс типа "-1"
                    # Это гарантирует, что ссылка на FunPay будет валидной
                    cleaned_order_id_match = re.search(r'([A-Za-z0-9]+)(?:-\d+)?', order_id)
                    cleaned_order_id = cleaned_order_id_match.group(1) if cleaned_order_id_match else order_id
                    
                    # Создаем словарь с данными заказа
                    order_data = {
                        'order_id': cleaned_order_id, # Используем очищенный ID
                        'chat_id': chat_id
                    }
                    # Вызываем функцию с правильными параметрами
                    send_order_completed_message(order_data, self.funpay_send_message_wrapper)
                except Exception as e:
                    print_flush(f"[FunPay][ERROR] Ошибка при отправке форматированного сообщения: {e}")
                return True
                
        return False
        
    def handle_new_message(self, event):
        try:
            import re
            from steam.steam_account_rental_utils import find_free_account, mark_account_rented, auto_end_rent
            from time import time
            from tg_utils.db import cleanup_expired_friend_modes, clear_friend_mode
            
            # Очищаем устаревшие режимы friend
            cleanup_expired_friend_modes()
            
            message = event.message
            text = message.text or ""
            chat_id = message.chat_id
            
            # Получаем информацию об отправителе
            author = None
            if hasattr(message, "author_username"):
                author = message.author_username
            elif hasattr(message, "author"):
                author = message.author
            elif hasattr(message, "from_user"):
                author = message.from_user.username or message.from_user.first_name
            else:
                author = "?"
            
            print_flush(f"[FunPay] Новое сообщение в чате: '{text}' (от {author}, чат {chat_id})")

            # Обработка команды !friend
            if text.strip().lower() == "!friend":
                try:
                    from tg_utils.db import set_friend_mode, is_friend_mode_active
                    
                    # Проверяем, не активирован ли уже режим
                    if is_friend_mode_active(chat_id):
                        self.funpay_send_message_wrapper(chat_id, "✅ Режим 'Для друга' уже активен! Действует 10 минут.")
                        return True
                    
                    # Активируем режим
                    set_friend_mode(chat_id)
                    self.funpay_send_message_wrapper(chat_id, "✅ Режим 'Для друга' включен! Действует 10 минут. При покупке нескольких лотов вы получите отдельные аккаунты.")
                    return True
                except Exception as e:
                    print_flush(f"[FunPay][ERROR] Ошибка при обработке команды !friend: {e}")
                    return False

            # --- Обработка заказа: сообщение от FunPay о покупке или выдаче аккаунта ---
            if author == "FunPay" and ("оплатил" in text.lower() or "аренд" in text.lower()):
                # Получаем order_id и chat_id
                order_match = re.search(r'#([A-Za-z0-9]+)', text)
                chat_id = message.chat_id
                order_id = order_match.group(1) if order_match else None
                
                # Проверяем количество купленных услуг
                quantity_match = re.search(r'(\d+)\s*шт', text)
                quantity = int(quantity_match.group(1)) if quantity_match else 1
                
                # Выводим для отладки исходный ID заказа и количество
                print_flush(f"[FunPay][MSG] Исходный ID заказа: {order_id}, количество: {quantity}")
                
                # Проверяем режим friend
                from tg_utils.db import is_friend_mode_active
                
                if is_friend_mode_active(chat_id):
                    print_flush(f"[FunPay][MSG] Режим 'Для друга' активен. Выдаём {quantity} отдельных аккаунтов.")
                    
                    # Определяем название игры из сообщения
                    game_name = None
                    for game in ["Counter-Strike: GO", "CS:GO", "CS GO", "CSGO"]:
                        if game.lower() in text.lower():
                            game_name = "Counter-Strike: GO"
                            break
                    
                    if not game_name:
                        print_flush(f"[FunPay][MSG] Не удалось определить игру из сообщения")
                        return False
                    
                    # Получаем время аренды из описания заказа
                    try:
                        details = self.account.get_order(order_id)
                        from steam.steam_account_rental_utils import parse_rent_time
                        desc_for_parse = details.full_description or details.short_description or ""
                        rent_seconds = parse_rent_time(desc_for_parse)
                        if not rent_seconds:
                            print_flush(f"[FunPay][MSG] Не удалось определить время аренды")
                            return False
                    except Exception as e:
                        print_flush(f"[FunPay][MSG] Ошибка при получении времени аренды: {e}")
                        return False
                    
                    # Находим нужное количество свободных аккаунтов
                    free_accounts = []
                    for _ in range(quantity):
                        acc = find_free_account(game_name)
                        if acc:
                            free_accounts.append(acc)
                        else:
                            break

                    if len(free_accounts) < quantity:
                        # Если недостаточно аккаунтов, отключаем режим friend и предлагаем продление
                        clear_friend_mode(chat_id)
                        
                        # Проверяем, есть ли уже арендованный аккаунт
                        from steam.steam_account_rental_utils import DB_PATH
                        import sqlite3
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute("SELECT id, rented_until, order_id FROM accounts WHERE status='rented' AND tg_user_id=?", (chat_id,))
                        acc_row = c.fetchone()
                        conn.close()
                        
                        if acc_row and order_id:
                            # Если есть арендованный аккаунт - продлеваем его
                            try:
                                details = self.account.get_order(order_id)
                                from steam.steam_account_rental_utils import parse_rent_time, mark_account_rented, auto_end_rent
                                desc_for_parse = details.full_description or details.short_description or ""
                                rent_seconds = parse_rent_time(desc_for_parse)
                                if rent_seconds:
                                    total_rent_seconds = rent_seconds * quantity
                                    new_until = mark_account_rented(acc_row[0], chat_id, bonus_seconds=total_rent_seconds, order_id=order_id)
                                    from steam.steam_account_rental_utils import format_msk_time
                                    end_time_msk = format_msk_time(new_until)
                                    message_text = f'✅ Аренда продлена до {end_time_msk}'
                                    self.funpay_send_message_wrapper(chat_id, message_text)
                                    remaining_time = new_until - time()
                                    auto_end_rent(
                                        acc_row[0], chat_id, remaining_time,
                                        notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                                    )
                            except Exception as e:
                                print_flush(f'[ERROR][MSG] Ошибка при продлении аренды: {e}')
                                traceback.print_exc(file=sys.stdout)
                                sys.stdout.flush()
                        else:
                            # Если нет арендованного аккаунта - выдаём доступные
                            for i, acc in enumerate(free_accounts):
                                try:
                                    acc_order_id = f"{order_id}-{i+1}" if order_id else None
                                    new_until = mark_account_rented(acc[0], chat_id, rented_until=time() + rent_seconds, order_id=acc_order_id)
                                    msg = (
                                        f"🎮 Аккаунт #{i+1}:\n\n"
                                        f"💼 Логин: {acc[1]}\n"
                                        f"🔑 Пароль: {acc[2]}\n\n"
                                        f"Для входа в аккаунт используйте клиент Steam."
                                    )
                                    self.funpay_send_message_wrapper(chat_id, msg)
                                    remaining_time = new_until - time()
                                    auto_end_rent(
                                        acc[0], chat_id, remaining_time,
                                        notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                                    )
                                    threading.Thread(target=self.send_steam_guard_code, args=(acc[0], chat_id, new_until), daemon=True).start()
                                except Exception as e:
                                    print_flush(f"[FunPay][ERROR] Ошибка при выдаче аккаунта #{i+1}: {e}")
                                    continue
                            
                            # Отправляем сообщение о недостатке аккаунтов
                            self.funpay_send_message_wrapper(chat_id, f"⚠️ Доступно только {len(free_accounts)} из {quantity} аккаунтов. Остальные будут выданы при освобождении.")
                        return True

                    for i, acc in enumerate(free_accounts):
                        try:
                            # Для каждого аккаунта создаём отдельный order_id
                            acc_order_id = f"{order_id}-{i+1}" if order_id else None
                            
                            # Арендуем аккаунт
                            new_until = mark_account_rented(acc[0], chat_id, rented_until=time() + rent_seconds, order_id=acc_order_id)
                            
                            # Отправляем данные аккаунта
                            msg = (
                                f"🎮 Аккаунт #{i+1}:\n\n"
                                f"💼 Логин: {acc[1]}\n"
                                f"🔑 Пароль: {acc[2]}\n\n"
                                f"Для входа в аккаунт используйте клиент Steam."
                            )
                            self.funpay_send_message_wrapper(chat_id, msg)
                            
                            # Запускаем таймер для каждого аккаунта
                            remaining_time = new_until - time()
                            auto_end_rent(
                                acc[0], chat_id, remaining_time,
                                notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                            )
                            
                            # Запускаем поиск Steam Guard кода
                            threading.Thread(target=self.send_steam_guard_code, args=(acc[0], chat_id, new_until), daemon=True).start()
                            
                        except Exception as e:
                            print_flush(f"[FunPay][ERROR] Ошибка при выдаче аккаунта #{i+1}: {e}")
                            continue

                    # Очищаем режим friend после успешной выдачи
                    clear_friend_mode(chat_id)
                    return True
                
                # Если режим friend не активен, проверяем продление
                # Проверяем, есть ли уже арендованный аккаунт на этот chat_id
                from steam.steam_account_rental_utils import DB_PATH
                import sqlite3
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT id, rented_until, order_id FROM accounts WHERE status='rented' AND tg_user_id=?", (chat_id,))
                acc_row = c.fetchone()
                conn.close()
                
                # Если аккаунт уже арендован этим пользователем и лот совпадает — продлеваем
                if acc_row and order_id:
                    try:
                        # Получаем описание лота для парсинга времени
                        details = self.account.get_order(order_id)
                        from steam.steam_account_rental_utils import parse_rent_time, mark_account_rented, auto_end_rent
                        from time import time
                        desc_for_parse = details.full_description or details.short_description or ""
                        rent_seconds = parse_rent_time(desc_for_parse)
                        if rent_seconds:
                            # Умножаем время аренды на количество купленных услуг
                            total_rent_seconds = rent_seconds * quantity
                            
                            # Теперь получаем новое время аренды из функции mark_account_rented
                            # Функция вернет фактическое время окончания аренды с учетом существующего времени
                            # Используем bonus_seconds для добавления времени к существующей аренде
                            new_until = mark_account_rented(acc_row[0], chat_id, bonus_seconds=total_rent_seconds, order_id=order_id)
                            
                            # Получаем дату и время окончания аренды в МСК 
                            from steam.steam_account_rental_utils import format_msk_time
                            end_time_msk = format_msk_time(new_until)
                            
                            # Формируем сообщение с информацией о времени окончания аренды
                            message_text = f'✅ Аренда продлена до {end_time_msk}'
                            
                            self.funpay_send_message_wrapper(chat_id, message_text)
                            
                            # Запускаем только auto_end_rent — он реализует таймер предупреждения и автоосвобождение
                            # Передаем оставшееся время до завершения аренды, а не полное время
                            remaining_time = new_until - time()
                            auto_end_rent(
                                acc_row[0], chat_id, remaining_time,
                                notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                            )
                        return
                    except Exception as e:
                        print_flush(f'[ERROR][MSG] Ошибка при продлении аренды: {e}')
                        import traceback
                        traceback.print_exc(file=sys.stdout)
                        sys.stdout.flush()
                        return

                import re
                # Инициализация для парсинга аренды
                lot_page = None
                details = None
                rent_seconds = None
                # Вывод описаний заказа и лота при оплате
                order_match = re.search(r'#([A-Za-z0-9]+)', text)
                if order_match:
                    order_id = order_match.group(1)
                    try:
                        details = self.account.get_order(order_id)
                        print_flush(f"[FunPay][MSG] Краткое описание заказа: {details.short_description}")
                        print_flush(f"[FunPay][MSG] Полное описание заказа: {details.full_description}")
                        # Парсим время аренды из описания заказа
                        try:
                            from steam.steam_account_rental_utils import parse_rent_time
                            desc_for_parse = details.full_description or details.short_description or ""
                            rent_seconds = parse_rent_time(desc_for_parse)
                            print_flush(f"[FunPay][MSG] Время аренды (сек): {rent_seconds}")
                        except Exception as e:
                            print_flush(f"[FunPay][MSG] Не удалось распарсить время аренды: {e}")
                    except Exception as e:
                        print_flush(f"[FunPay][MSG] Ошибка при получении деталей заказа: {e}")
                        
                def is_rent_order(description: str) -> bool:
                    rent_keywords = ["аренда", "rent"]
                    desc = description.lower()
                    return any(word in desc for word in rent_keywords)

                is_rent = False

                # 1. Проверяем подкатегорию
                if details and hasattr(details, 'subcategory') and details.subcategory:
                    subcat_name = getattr(details.subcategory, 'name', '').lower()
                    if 'аренда' in subcat_name or 'rent' in subcat_name:
                        is_rent = True

                # 2. Проверяем параметры
                if not is_rent and details and hasattr(details, 'params') and details.params:
                    params_str = str(details.params).lower()
                    if 'аренда' in params_str or 'rent' in params_str:
                        is_rent = True

                # 3. Проверяем описание (старый способ)
                if not is_rent:
                    desc_for_parse = (details.full_description or "") + " " + (details.short_description or "") if details else text
                    is_rent = is_rent_order(desc_for_parse)

                # 4. Проверяем текст сообщения (старый способ)
                if not is_rent:
                    is_rent = 'аренд' in text.lower() or 'rent' in text.lower()

                if not is_rent:
                    print_flush("[FunPay][MSG] В заказе нет признаков аренды, пропускаем выдачу аккаунта.")
                    return

                game_name = None
                
                game_match = re.search(r'(CS:GO|Counter-Strike:? ?GO?|КС:ГО|Контра|Каэс)', text, re.IGNORECASE)
                if game_match:
                    game_name = "Counter-Strike: GO"
                    
                # 2. Проверяем Red Dead Redemption 2 и другие популярные игры
                elif re.search(r'(Red Dead|RDR2|Redemption)', text, re.IGNORECASE):
                    game_name = "Red Dead Redemption 2"
                elif re.search(r'(GTA|Grand Theft Auto)', text, re.IGNORECASE):
                    game_name = "Grand Theft Auto V"
                
                if not game_name:
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute("SELECT DISTINCT game_name FROM accounts WHERE status='free'")
                        available_games = [row[0] for row in c.fetchall()]
                        conn.close()
                        
                        # Ищем совпадение в тексте сообщения
                        for game in available_games:
                            if game.lower() in text.lower():
                                game_name = game
                                print_flush(f"[FunPay][MSG] Найдена игра в тексте сообщения: {game}")
                                break
                                
                        if not game_name and details:
                            desc_text = (details.short_description or '') + ' ' + (details.full_description or '')
                            for game in available_games:
                                if game.lower() in desc_text.lower():
                                    game_name = game
                                    print_flush(f"[FunPay][MSG] Найдена игра в описании заказа: {game}")
                                    break
                                    
                        if not game_name:
                            combined_text = text + ' ' + (details.short_description or '') + ' ' + (details.full_description or '') if details else text
                            for game in available_games:
                                game_words = [w for w in game.lower().split() if len(w) > 3]
                                for word in game_words:
                                    if word in combined_text.lower():
                                        game_name = game
                                        print_flush(f"[FunPay][MSG] Найдена игра по ключевому слову '{word}': {game}")
                                        break
                                if game_name:
                                    break
                    except Exception as e:
                        print_flush(f"[FunPay][MSG] Ошибка при поиске игры в БД: {e}")
                
                if game_name:
                    print_flush(f"[FunPay][MSG] Обнаружена аренда игры: {game_name}")
                    from steam.steam_account_rental_utils import find_free_account, mark_account_rented, auto_end_rent
                    acc = find_free_account(game_name)
                    if not acc:
                        self.funpay_send_message_wrapper(message.chat_id, f'Нет свободных аккаунтов для игры {game_name}.')
                        print_flush(f'[FunPay][MSG] Нет свободных аккаунтов для {game_name}')
                        return
                    try:
                        if rent_seconds is None:
                            print_flush(f"[FunPay][MSG] Не удалось определить время аренды из описания заказа: {details.full_description}")
                            return
                        # Теперь time() будет доступна здесь
                        from time import time
                        
                        # Умножаем время аренды на количество купленных услуг
                        total_rent_seconds = rent_seconds * quantity
                        
                        # Проверяем режим friend
                        from tg_utils.db import is_friend_mode_active, clear_friend_mode
                        
                        if is_friend_mode_active(chat_id) and quantity > 1:
                            # Если включен режим friend и куплено больше 1 лота
                            print_flush(f"[FunPay][MSG] Режим 'Для друга' активен. Выдаём {quantity} отдельных аккаунтов.")
                            
                            # Находим нужное количество свободных аккаунтов
                            free_accounts = []
                            for _ in range(quantity):
                                acc = find_free_account(game_name)
                                if acc:
                                    free_accounts.append(acc)
                                else:
                                    break

                            if len(free_accounts) < quantity:
                                # Если недостаточно аккаунтов, отключаем режим friend и предлагаем продление
                                clear_friend_mode(chat_id)
                                
                                # Проверяем, есть ли уже арендованный аккаунт
                                conn = sqlite3.connect(DB_PATH)
                                c = conn.cursor()
                                c.execute("SELECT id, rented_until, order_id FROM accounts WHERE status='rented' AND tg_user_id=?", (chat_id,))
                                acc_row = c.fetchone()
                                conn.close()
                                
                                if acc_row and order_id:
                                    # Если есть арендованный аккаунт - продлеваем его
                                    try:
                                        details = self.account.get_order(order_id)
                                        from steam.steam_account_rental_utils import parse_rent_time, mark_account_rented, auto_end_rent
                                        desc_for_parse = details.full_description or details.short_description or ""
                                        rent_seconds = parse_rent_time(desc_for_parse)
                                        if rent_seconds:
                                            total_rent_seconds = rent_seconds * quantity
                                            new_until = mark_account_rented(acc_row[0], chat_id, bonus_seconds=total_rent_seconds, order_id=order_id)
                                            from steam.steam_account_rental_utils import format_msk_time
                                            end_time_msk = format_msk_time(new_until)
                                            message_text = f'✅ Аренда продлена до {end_time_msk}'
                                            self.funpay_send_message_wrapper(chat_id, message_text)
                                            remaining_time = new_until - time()
                                            auto_end_rent(
                                                acc_row[0], chat_id, remaining_time,
                                                notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                                            )
                                    except Exception as e:
                                        print_flush(f'[ERROR][MSG] Ошибка при продлении аренды: {e}')
                                        traceback.print_exc(file=sys.stdout)
                                        sys.stdout.flush()
                                else:
                                    # Если нет арендованного аккаунта - выдаём доступные
                                    for i, acc in enumerate(free_accounts):
                                        try:
                                            acc_order_id = f"{order_id}-{i+1}" if order_id else None
                                            new_until = mark_account_rented(acc[0], chat_id, rented_until=time() + rent_seconds, order_id=acc_order_id)
                                            msg = (
                                                f"🎮 Аккаунт #{i+1}:\n\n"
                                                f"💼 Логин: {acc[1]}\n"
                                                f"🔑 Пароль: {acc[2]}\n\n"
                                                f"Для входа в аккаунт используйте клиент Steam."
                                            )
                                            self.funpay_send_message_wrapper(chat_id, msg)
                                            remaining_time = new_until - time()
                                            auto_end_rent(
                                                acc[0], chat_id, remaining_time,
                                                notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                                            )
                                            threading.Thread(target=self.send_steam_guard_code, args=(acc[0], chat_id, new_until), daemon=True).start()
                                        except Exception as e:
                                            print_flush(f"[FunPay][ERROR] Ошибка при выдаче аккаунта #{i+1}: {e}")
                                            continue
                                    
                                    # Отправляем сообщение о недостатке аккаунтов
                                    self.funpay_send_message_wrapper(chat_id, f"⚠️ Доступно только {len(free_accounts)} из {quantity} аккаунтов. Остальные будут выданы при освобождении.")
                                return True

                            # Выдаём каждый аккаунт отдельно
                            for i, acc in enumerate(free_accounts):
                                try:
                                    # Для каждого аккаунта создаём отдельный order_id
                                    acc_order_id = f"{order_id}-{i+1}" if order_id else None
                                    
                                    # Арендуем аккаунт
                                    new_until = mark_account_rented(acc[0], chat_id, rented_until=time() + rent_seconds, order_id=acc_order_id)
                                    
                                    # Отправляем данные аккаунта
                                    msg = (
                                        f"🎮 Аккаунт #{i+1}:\n\n"
                                        f"💼 Логин: {acc[1]}\n"
                                        f"🔑 Пароль: {acc[2]}\n\n"
                                        f"Для входа в аккаунт используйте клиент Steam."
                                    )
                                    self.funpay_send_message_wrapper(chat_id, msg)
                                    
                                    # Запускаем таймер для каждого аккаунта
                                    remaining_time = new_until - time()
                                    auto_end_rent(
                                        acc[0], chat_id, remaining_time,
                                        notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                                    )
                                    
                                    # Запускаем поиск Steam Guard кода
                                    threading.Thread(target=self.send_steam_guard_code, args=(acc[0], chat_id, new_until), daemon=True).start()
                                    
                                except Exception as e:
                                    print_flush(f"[FunPay][ERROR] Ошибка при выдаче аккаунта #{i+1}: {e}")
                                    continue

                            # Очищаем режим friend после успешной выдачи
                            clear_friend_mode(chat_id)
                            return True

                        # Используем новую версию mark_account_rented, которая возвращает точное время окончания
                        # При новой аренде передаем rented_until как абсолютный timestamp
                        new_until = mark_account_rented(acc[0], message.chat_id, rented_until=time() + total_rent_seconds, order_id=order_id)
                        
                        # Логируем с учетом количества
                        if quantity > 1:
                            print_flush(f'[FunPay][MSG] Аккаунт {acc[0]} арендован на {total_rent_seconds // 60} минут ({quantity} шт. x {rent_seconds // 60} минут).')
                        else:
                            print_flush(f'[FunPay][MSG] Аккаунт {acc[0]} арендован на {total_rent_seconds // 60} минут.')
                        
                        # --- Запускаем таймеры для предупреждения и автоосвобождения ---
                        # Передаем точное время до окончания аренды
                        remaining_time = new_until - time()
                        auto_end_rent(
                            acc[0], message.chat_id, remaining_time,
                            notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message(tg_user_id)
                        )
                    except Exception as e:
                        print_flush(f'[ERROR][MSG] Не удалось пометить аккаунт как занятый: {e}')
                        import traceback
                        traceback.print_exc(file=sys.stdout)
                        sys.stdout.flush()
                    login, password, game_name_db = acc[1], acc[2], acc[3]
                    
                    # Убираем информацию о длительности аренды из сообщения
                    msg = (
                        f"🎮 Ваш арендованный Steam-аккаунт:\n\n"
                        f"💼 Логин: {login}\n"
                        f"🔑 Пароль: {password}\n\n"
                        f"Для входа в аккаунт используйте клиент Steam."
                    )
                    try:
                        self.funpay_send_message_wrapper(message.chat_id, msg)
                        print_flush(f'[FunPay][MSG] Аккаунт {acc[0]} выдан.')
                        # --- Через 3 секунды ищем Steam Guard код и отправляем клиенту ---
                        def send_steam_guard_code():
                            from utils.logger import logger as utils_logger
                            print_flush(f"[FunPay][STEAM GUARD] Начинаем поиск Steam Guard кода для аккаунта {acc[0]}")
                            from time import sleep
                            sleep(3)
                            
                            # Проверяем настройку steam_guard_enabled
                            import sqlite3
                            from steam.steam_account_rental_utils import DB_PATH
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc[0],))
                            steam_guard_enabled = c.fetchone()[0]
                            conn.close()
                            
                            if not steam_guard_enabled:
                                print_flush(f"[FunPay][STEAM GUARD] Поиск кода отключен для аккаунта {acc[0]}")
                                return
                            
                            # Получаем почтовые данные из БД по ID аккаунта
                            from db.accounts import get_account_by_id
                            _, _, email_login, email_password, imap_host = get_account_by_id(acc[0])
                            if not (email_login and email_password and imap_host):
                                print_flush(f"[FunPay][STEAM GUARD] Нет почтовых данных для аккаунта {acc[0]}")
                                return
                            
                            try:
                                from utils.email_utils import fetch_steam_guard_code_from_email
                                code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, logger=utils_logger)
                                if code:
                                    # Проверяем, что это не автоматический вход при окончании аренды
                                    import sqlite3
                                    from steam.steam_account_rental_utils import DB_PATH
                                    conn = sqlite3.connect(DB_PATH)
                                    c = conn.cursor()
                                    c.execute("SELECT status, rented_until, order_id FROM accounts WHERE id=?", (acc[0],))
                                    row = c.fetchone()
                                    conn.close()
                                    
                                    # Если аккаунт в статусе rented и rented_until скоро будет истекать, вероятно это auto_end_rent
                                    import time
                                    current_time = time.time()
                                    is_auto_end_rent = False
                                    
                                    if row and row[0] == 'rented' and row[1] is not None:
                                        remaining_time = float(row[1]) - current_time
                                        if remaining_time <= 60:  # Если осталось меньше минуты
                                            is_auto_end_rent = True
                                            print_flush(f"[FunPay][STEAM GUARD] Код {code} не будет отправлен клиенту, так как это автоматическое окончание аренды")
                                    
                                    # Отправляем код только если это не auto_end_rent
                                    if not is_auto_end_rent:
                                        # Используем новую функцию с форматированным сообщением
                                        from steam.steam_account_rental_utils import send_steam_guard_code
                                        # Получаем timestamp окончания аренды
                                        rented_until = float(row[1]) if row and row[1] is not None else (current_time + 3600)  # Если нет - ставим 1 час
                                        
                                        # Функция-обёртка для self.account.send_message, обеспечивающая правильную передачу параметров
                                        def send_msg_wrapper(chat_id, text):
                                            try:
                                                self.funpay_send_message_wrapper(chat_id, text)
                                            except Exception as e:
                                                print_flush(f"[FunPay][ERROR] Не удалось отправить сообщение: {e}")
                                        
                                        send_steam_guard_code(message.chat_id, code, rented_until, send_msg_wrapper)
                                        print_flush(f"[FunPay][STEAM GUARD] Код {code} отправлен клиенту для аккаунта {acc[0]}")
                                else:
                                    print_flush(f"[FunPay][STEAM GUARD] Код не найден для аккаунта {acc[0]}")
                            except Exception as e:
                                print_flush(f"[FunPay][STEAM GUARD] Ошибка при поиске Steam Guard кода для аккаунта {acc[0]}: {e}")
                                traceback.print_exc(file=sys.stdout)
                                sys.stdout.flush()
                        threading.Thread(target=send_steam_guard_code, daemon=True).start()
                    except Exception as e:
                        print_flush(f'[ERROR][MSG] Не удалось отправить данные аккаунта клиенту: {e}')
                        import traceback
                        traceback.print_exc(file=sys.stdout)
                        sys.stdout.flush()
                    return
                else:
                    # Не удалось определить игру по сообщению
                    print_flush(f"[FunPay][MSG] Не удалось определить игру из сообщения: {text}")
                    try:
                        # Получаем список доступных игр
                        conn = sqlite3.connect(DB_PATH)
                        c = conn.cursor()
                        c.execute("SELECT DISTINCT game_name FROM accounts WHERE status='free'")
                        available_games = [row[0] for row in c.fetchall()]
                        conn.close()
                        
                        if available_games:
                            games_list = ", ".join(available_games)
                            print_flush(f"[FunPay][MSG] Доступные игры: {games_list}")
                            # Можно отправить список доступных игр клиенту, но это опционально
                    except Exception as e:
                        print_flush(f"[FunPay][MSG] Ошибка при получении списка игр: {e}")
            # --- Тестовая команда: если сообщение содержит 'дай' ---
            if "дай" in text.lower():
                # Проверяем, что команда от определенного пользователя
                allowed_users = ["dadayaredaze"]
                if author.lower() not in [user.lower() for user in allowed_users]:
                    print_flush(f"[FunPay][TEST] Команда 'дай' от неавторизованного пользователя {author}. Игнорируем.")
                    return
                
                # Проверяем, есть ли указание на количество в сообщении
                quantity_match = re.search(r'дай\s+(\d+)', text.lower())
                test_quantity = int(quantity_match.group(1)) if quantity_match else 1
                
                game_name = "Counter-Strike: GO"
                print_flush(f"[FunPay][TEST] Команда 'дай' от {author}. Выдаём тестовый аккаунт {game_name} на {test_quantity} минут(ы).")
                from steam.steam_account_rental_utils import find_free_account, mark_account_rented, auto_end_rent
                acc = find_free_account(game_name)
                if not acc:
                    self.funpay_send_message_wrapper(message.chat_id, "Нет свободных аккаунтов для теста.")
                    return
                login, password, game_name_db = acc[1], acc[2], acc[3]
                # Пометить аккаунт как арендованный на указанное количество минут
                from time import time
                # Для тестового режима используем специальный префикс
                # Передаем длительность аренды в bonus_seconds
                new_until = mark_account_rented(acc[0], message.chat_id, bonus_seconds=60 * test_quantity, order_id=f"TEST-{acc[0]}")

                # Убираем информацию о длительности аренды из сообщения
                msg = (
                    f"🎮 Ваш арендованный Steam-аккаунт:\n\n"
                    f"💼 Логин: {login}\n"
                    f"🔑 Пароль: {password}\n\n"
                    f"Для входа в аккаунт используйте клиент Steam."
                )
                self.funpay_send_message_wrapper(message.chat_id, msg)
                # Steam Guard код (если есть)
                def send_steam_guard_code_test():
                    from utils.logger import logger as utils_logger
                    print_flush(f"[FunPay][STEAM GUARD TEST] Ищу Steam Guard кода для аккаунта {acc[0]}")
                    from time import sleep
                    sleep(3)
                    from db.accounts import get_account_by_id
                    _, _, email_login, email_password, imap_host = get_account_by_id(acc[0])
                    if not (email_login and email_password and imap_host):
                        print_flush(f"[FunPay][STEAM GUARD TEST] Нет почтовых данных для аккаунта {acc[0]}")
                        return
                    try:
                        from utils.email_utils import fetch_steam_guard_code_from_email
                        # Ищем код в течение 10 минут
                        print_flush(f"[FunPay][STEAM GUARD TEST] Запускаем поиск кода с таймаутом 600 секунд (10 минут)")
                        code = fetch_steam_guard_code_from_email(email_login, email_password, imap_host, logger=utils_logger)
                        if code:
                            # Используем новую функцию с форматированным сообщением
                            from steam.steam_account_rental_utils import send_steam_guard_code
                            # Для тестовой аренды на указанное количество минут
                            current_time = time()

                            # Функция-обёртка для self.account.send_message
                            def send_msg_wrapper(chat_id, text):
                                try:
                                    self.funpay_send_message_wrapper(chat_id, text)
                                except Exception as e:
                                    print_flush(f"[FunPay][ERROR] Не удалось отправить тестовое сообщение: {e}")

                            send_steam_guard_code(message.chat_id, code, current_time + (test_quantity * 60), send_msg_wrapper)
                            print_flush(f"[FunPay][STEAM GUARD TEST] Код {code} отправлен клиенту для аккаунта {acc[0]}")
                        else:
                            print_flush(f"[FunPay][STEAM GUARD TEST] Код не найден для аккаунта {acc[0]} после 10 минут поиска")
                    except Exception as e:
                        print_flush(f"[FunPay][STEAM GUARD TEST] Ошибка при поиске Steam Guard кода для аккаунта {acc[0]}: {e}")
                        traceback.print_exc(file=sys.stdout)
                        sys.stdout.flush()
                threading.Thread(target=send_steam_guard_code_test, daemon=True).start()
                # Запускаем автоосвобождение и смену данных через указанное количество минут
                try:
                    print_flush(f"[FunPay][TEST] Запускаем тестовую аренду на {test_quantity * 60} секунд для аккаунта {acc[0]}")
                    auto_end_rent(acc[0], message.chat_id, test_quantity * 60,
                                 notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message({'chat_id': tg_user_id, 'order_id': f"TEST-{acc[0]}"}, lambda chat_id, text: self.funpay_send_message_wrapper(chat_id, text)))
                    print_flush(f"[FunPay][TEST] Таймер завершения аренды успешно запущен для аккаунта {acc[0]}")
                except Exception as e:
                    print_flush(f"[FunPay][TEST] Не удалось запустить авто-завершение тестовой аренды: {e}")
                    traceback.print_exc(file=sys.stdout)
                    sys.stdout.flush()
                return
            if 'wassupbeijing' in text.lower():
                self.funpay_send_message_wrapper(message.chat_id, 'wassup!')
            # --- Обработка команды !check ---
            if text.strip().lower().startswith("!check"):
                parts = text.strip().split(" ", 1)
                if len(parts) < 2:
                    self.funpay_send_message_wrapper(message.chat_id, "Пожалуйста, укажите название игры после !check")
                    return
                game_query = parts[1].strip() # Сохраняем оригинальный запрос игры для сообщения
                game_query_lower = game_query.lower() # Используем нижний регистр для поиска
                
                import sqlite3
                from steam.steam_account_rental_utils import DB_PATH, format_msk_time # Импортируем format_msk_time
                import time # Импортируем time
                
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                
                # Сначала ищем свободные аккаунты по частичному совпадению (регистр не важен)
                c.execute("SELECT game_name, COUNT(*) FROM accounts WHERE status='free' AND LOWER(game_name) LIKE ?", (f"%{game_query_lower}%",))
                row_free = c.fetchone()
                
                if row_free and row_free[1] > 0:
                    # Есть свободные аккаунты
                    count = row_free[1]
                    game_name = row_free[0] # Используем название игры из БД
                    if count == 1:
                        msg = f"Есть свободный аккаунт для игры {game_name}"
                    else:
                        msg = f"{count} свободных аккаунтов для игры {game_name}"
                else:
                    # Нет свободных аккаунтов, ищем ближайший занятый для этой игры
                    c.execute("""
                        SELECT rented_until, game_name 
                        FROM accounts 
                        WHERE LOWER(game_name) LIKE ? AND status='rented' 
                        ORDER BY rented_until ASC 
                        LIMIT 1
                    """, (f"%{game_query_lower}%",))
                    row_rented = c.fetchone()
                    
                    if row_rented and row_rented[0] is not None:
                        # Найден занятый аккаунт, сообщаем, когда он освободится
                        rented_until_timestamp = float(row_rented[0])
                        game_name_rented = row_rented[1] # Используем название игры из БД
                        
                        # Проверяем, что время освобождения в будущем
                        if rented_until_timestamp > time.time():
                            free_time_msk = format_msk_time(rented_until_timestamp)
                            msg = f"Нет свободных аккаунтов для игры {game_name_rented}. Ближайший освободится примерно {free_time_msk}."
                        else:
                            # Аккаунт должен был уже освободиться, но статус не сброшен
                            msg = f"Нет свободных аккаунтов для игры {game_query}. Пожалуйста, попробуйте позже."
                            # Возможно, здесь стоит добавить логирование или уведомление администратору
                            print_flush(f"[FunPay][MSG][WARN] Аккаунт для игры '{game_query_lower}' должен был освободиться, но статус 'rented'. acc_id: ???") # TODO: добавить acc_id в запрос если нужно
                            
                    else:
                        # Нет ни свободных, ни занятых аккаунтов для этого запроса
                        msg = f"Нет свободных аккаунтов для игры {game_query}."
                
                conn.close()
                self.funpay_send_message_wrapper(message.chat_id, msg)
                return
        except Exception as e:
            print_flush(f"[FunPay] Ошибка в обработчике нового сообщения: {e}")
            import traceback
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()

    def handle_review_message(self, event):
        """
        Обработка сообщений, связанных с отзывами.
        
        Args:
            event: Событие нового сообщения
            
        Returns:
            bool: True, если сообщение связано с отзывом и обработано, False в противном случае
        """
        try:
            # Получаем данные сообщения
            message = event.message
            text = message.text or ""
            chat_id = message.chat_id
            author = getattr(message, "author_username", None) or getattr(message, "author", None) or "?"

            # Проверяем, является ли сообщение уведомлением об отзыве от FunPay
            if author != "FunPay":
                # НЕ БЛОКИРУЕМ обработку - просто возвращаем False и позволяем другим обработчикам видеть сообщение
                return False

            # Инициализируем флаг, что сообщение является отзывом
            is_review_message = False
            
            # Импортируем re внутри функции для избежания ошибок области видимости
            import re
            
            try:
                regex_patterns = RegularExpressions()
                # Проверяем на новый отзыв с RegularExpressions из FunPayAPI
                if regex_patterns.NEW_FEEDBACK.search(text) or regex_patterns.FEEDBACK_CHANGED.search(text):
                    is_review_message = True
            except Exception as e:
                print_flush(f"[FunPay][REVIEW] Ошибка при создании RegularExpressions: {e}")
                # Используем встроенные регулярные выражения
                re_new_feedback = re.compile(
                    r"(Покупатель|The buyer) [a-zA-Z0-9]+ (написал отзыв к заказу|has given feedback to the order) #[A-Z0-9]{8}\."
                )
                re_feedback_changed = re.compile(
                    r"(Покупатель|The buyer) [a-zA-Z0-9]+ (изменил отзыв к заказу|has edited their feedback to the order) #[A-Z0-9]{8}\."
                )
                
                # Проверяем на новый отзыв с нашими локальными регулярными выражениями
                if re_new_feedback.search(text) or re_feedback_changed.search(text):
                    is_review_message = True
            
            # Если сообщение не является отзывом, НЕ БЛОКИРУЕМ обработку
            if not is_review_message:
                return False
                
            print_flush(f"[FunPay][REVIEW] Обнаружен новый отзыв: {text}")
            
            # Получаем ID заказа из сообщения
            order_match = re.search(r'#([A-Za-z0-9]{8})', text)
            if not order_match:
                print_flush(f"[FunPay][REVIEW] Не удалось получить ID заказа из сообщения: {text}")
                return False
            order_id = order_match.group(1)
            print_flush(f"[FunPay][REVIEW] ID заказа: {order_id}")
            
            # Получаем информацию об арендованном аккаунте
            account_id, rented_until, account_chat_id = self.get_rented_account_by_order_id(order_id)
            if not account_id:
                print_flush(f"[FunPay][REVIEW] Не найден арендованный аккаунт для заказа {order_id}")
                return True
            target_chat_id = account_chat_id or chat_id

            # --- ПРОВЕРКА И ВЫДАЧА БОНУСА ---
            # Проверяем, был ли уже выдан бонус за этот заказ
            import sqlite3
            from steam.steam_account_rental_utils import DB_PATH
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("PRAGMA table_info(accounts)")
            columns = [column[1] for column in c.fetchall()]
            bonus_given = 0
            if 'bonus_given' in columns:
                c.execute("SELECT bonus_given FROM accounts WHERE id=?", (account_id,))
                row = c.fetchone()
                if row and row[0]:
                    bonus_given = int(row[0])
            conn.close()
            if bonus_given:
                print_flush(f"[FunPay][REVIEW] Бонус уже был выдан для аккаунта {account_id} (order_id={order_id})")
                return True

            # Получаем информацию о заказе через API FunPay
            try:
                order_details = self.account.get_order(order_id)
                if not order_details or not hasattr(order_details, 'review') or not order_details.review:
                    print_flush(f"[FunPay][REVIEW] Не удалось получить детали отзыва для заказа {order_id}")
                    return True
                    
                # Проверяем рейтинг отзыва (положительный/отрицательный)
                stars = getattr(order_details.review, 'stars', 0)
                review_text = getattr(order_details.review, 'text', '').strip()
                
                print_flush(f"[FunPay][REVIEW] Отзыв для заказа {order_id}: {stars} звезд, текст: {review_text}")
                
                # Проверяем, является ли отзыв положительным (4-5 звезд)
                if stars >= 4:
                    # Добавляем бонусное время к аренде только если она ещё активна
                    if rented_until:
                        current_time = time()
                        # Проверяем, что аренда ещё не закончилась
                        if float(rented_until) > current_time:
                            # --- ВЫДАЧА БОНУСА ---
                            from steam.steam_account_rental_utils import mark_account_rented, auto_end_rent, format_msk_time
                            new_until = mark_account_rented(account_id, target_chat_id, bonus_seconds=REVIEW_BONUS_TIME, order_id=order_id)
                            # Ставим флаг bonus_given=1
                            conn = sqlite3.connect(DB_PATH)
                            c = conn.cursor()
                            if 'bonus_given' in columns:
                                c.execute("UPDATE accounts SET bonus_given=1 WHERE id=?", (account_id,))
                                conn.commit()
                            conn.close()

                            # Получаем форматированное время окончания аренды
                            end_time_msk = format_msk_time(new_until)

                            # Отправляем сообщение о бонусе
                            bonus_message = (
                                f"🎁 Спасибо за положительный отзыв! 🎁\n\n"
                                f"✅ Мы добавили {REVIEW_BONUS_TIME // 60} минут бонусного времени к вашей аренде!\n"
                                f"⏱️ Новое время окончания: {end_time_msk}\n\n"
                                f"Приятной игры! 🎮"
                            )
                            self.funpay_send_message_wrapper(target_chat_id, bonus_message)
                            
                            # Обновляем автозавершение
                            remaining_time = new_until - time()
                            if remaining_time > 0:
                                auto_end_rent(
                                    account_id, target_chat_id, remaining_time,
                                    notify_callback=lambda acc_id, tg_user_id: self.send_order_completed_message({'chat_id': tg_user_id, 'order_id': order_id}, lambda chat_id, text: self.funpay_send_message_wrapper(chat_id, text))
                                )
                        else:
                            # Аренда уже закончилась, просто логируем событие без действий
                            print_flush(f"[FunPay][REVIEW] Аренда для заказа {order_id} уже закончилась, бонусное время не добавлено")
                    else:
                        print_flush(f"[FunPay][REVIEW] Не удалось определить время окончания аренды для заказа {order_id}")
                elif stars > 0:
                    # Отрицательный отзыв, просто логируем без ответа
                    print_flush(f"[FunPay][REVIEW] Получен отрицательный отзыв для заказа {order_id}: {stars} звезд, текст: {review_text}")
                
                # Возвращаем True, если это отзыв
                return is_review_message
                
            except Exception as e:
                print_flush(f"[FunPay][REVIEW] Ошибка при получении/обработке отзыва: {e}")
                traceback.print_exc(file=sys.stdout)
                sys.stdout.flush()
            
            return is_review_message
            
        except Exception as e:
            print_flush(f"[FunPay][REVIEW] Неожиданная ошибка при обработке сообщения об отзыве: {e}")
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()
            # В случае ошибки не блокируем обработку сообщения
            return False

    @staticmethod
    def add_game_name(raw_name, normalized_name=None):
        from game_name_mapper import mapper
        mapper.add_game(raw_name, normalized_name)

    @staticmethod
    def parse_game_from_description(description):
        # Простейший парсер: берём первое слово до запятой или двоеточия
        import re
        match = re.match(r'([\w\- ]+?)[,: ]', description)
        if match:
            return match.group(1).strip()
        return description.split(',')[0].strip() if ',' in description else description.split(':')[0].strip()

    @staticmethod
    def parse_rent_duration(description):
        import re
        match = re.search(r'(\d+)[ ]*(час|ч|h)', description, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 12  # по умолчанию 12 часов

    def send_order_completed_message(self, message_chat_id, order_id=None):
        """
        Отправляет сообщение о выполнении заказа
        
        Args:
            message_chat_id (str): ID чата для отправки сообщения
            order_id (str, optional): ID заказа, если известен
        """
        try:
            # Если order_id не передан или это тестовый/телеграм ID, пробуем найти настоящий ID заказа
            if not order_id or order_id.startswith('TG-') or order_id.startswith('TEST-'):
                try:
                    from steam.steam_account_rental_utils import DB_PATH
                    import sqlite3
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    # Проверяем наличие столбца order_id в таблице accounts
                    c.execute("PRAGMA table_info(accounts)")
                    columns = [column[1] for column in c.fetchall()]
                    
                    if 'order_id' in columns:
                        # Ищем order_id по chat_id (tg_user_id) и статусу 'rented'
                        c.execute("SELECT order_id FROM accounts WHERE tg_user_id=? AND status='rented'", (message_chat_id,))
                        row = c.fetchone()
                        if row and row[0] and not (row[0].startswith('TG-') or row[0].startswith('TEST-')):
                            order_id = row[0]
                            print_flush(f"[FunPay][ORDER] Найден ID заказа в БД: {order_id}")
                    conn.close()
                except Exception as e:
                    print_flush(f"[ERROR] Не удалось получить ID заказа из базы данных: {e}")
            
            # Если до сих пор нет order_id, используем UNKNOWN
            real_order_id = order_id or 'UNKNOWN'
            
            # Выводим для отладки финальный ID
            print_flush(f"[FunPay][ORDER] Используется ID заказа: {real_order_id}")
            
            # Создаем объект заказа с необходимыми данными
            order_data = {'chat_id': message_chat_id, 'order_id': real_order_id}
            
            # Используем общую функцию из модуля steam_account_rental_utils
            from steam.steam_account_rental_utils import send_order_completed_message
            send_order_completed_message(order_data, self.funpay_send_message_wrapper)
        except Exception as e:
            print_flush(f"[FunPay][ERROR] Не удалось отправить сообщение о выполнении заказа: {e}")

    def funpay_send_message_wrapper(self, chat_id, text):
        """
        Обертка для отправки сообщений в FunPay без HTML-тегов.
        
        Args:
            chat_id: ID чата FunPay
            text: Текст сообщения, возможно с HTML-тегами
        """
        try:
            # Удаляем HTML-теги <code> и </code>
            clean_text = text.replace("<code>", "").replace("</code>", "")
            # Исправляем экранированные переносы строк
            clean_text = clean_text.replace("\\n", "\n").replace('\r\n', '\n').replace('\r', '\n')
            self.account.send_message(chat_id, clean_text)
            return True
        except Exception as e:
            print_flush(f"[ERROR] Ошибка при отправке сообщения в FunPay: {e}")
            return False

    def get_rented_account_by_order_id(self, order_id):
        """
        Получает информацию об арендованном аккаунте по ID заказа.
        
        Args:
            order_id (str): ID заказа FunPay
            
        Returns:
            tuple: (account_id, rented_until, chat_id) или (None, None, None), если аккаунт не найден
        """
        try:
            from steam.steam_account_rental_utils import DB_PATH
            import sqlite3
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Проверяем наличие столбца order_id
            c.execute("PRAGMA table_info(accounts)")
            columns = [column[1] for column in c.fetchall()]
            
            if 'order_id' in columns:
                c.execute("SELECT id, rented_until, tg_user_id FROM accounts WHERE order_id=? AND status='rented'", (order_id,))
                row = c.fetchone()
                conn.close()
                
                if row:
                    return row[0], row[1], row[2]
            else:
                conn.close()
                print_flush(f"[FunPay][ERROR] В таблице accounts отсутствует столбец order_id")
        except Exception as e:
            print_flush(f"[FunPay][ERROR] Ошибка при получении информации об аккаунте по ID заказа {order_id}: {e}")
            traceback.print_exc(file=sys.stdout)
            sys.stdout.flush()
        
        return None, None, None
