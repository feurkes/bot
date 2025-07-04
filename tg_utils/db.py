import sqlite3
import os
import time
import logging
from steam.steam_account_rental_utils import send_order_completed_message
from funpay_integration import FunPayListener
from config import DB_PATH, DB_DIR # Импортируем из нового файла config.py

# Удаляем старые определения DB_DIR и DB_PATH
# DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../storage/plugins')
# DB_PATH = os.path.join(DB_DIR, 'steam_rental.db')
logger = logging.getLogger("steam_rental")

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS accounts (
        id TEXT PRIMARY KEY,
        login TEXT NOT NULL,
        password TEXT NOT NULL,
        game_name TEXT NOT NULL,
        rented_until INTEGER,
        status TEXT NOT NULL DEFAULT 'free',
        tg_user_id INTEGER,
        email_login TEXT,
        email_password TEXT,
        imap_host TEXT,
        order_id TEXT,
        steam_guard_enabled INTEGER DEFAULT 1,
        warned_10min INTEGER DEFAULT 0,
        bonus_given INTEGER DEFAULT 0,
        lot_id TEXT
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS authorized_users (
        user_id INTEGER PRIMARY KEY,
        is_authorized INTEGER DEFAULT 0,
        access_attempts INTEGER DEFAULT 0,
        last_attempt INTEGER
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS friend_mode_settings (
        tg_user_id INTEGER PRIMARY KEY,
        activated_at INTEGER NOT NULL,
        is_active INTEGER DEFAULT 0
    )''')
    conn.commit()
    conn.close()

def ensure_accounts_columns():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Проверяем существующие колонки
    c.execute("PRAGMA table_info(accounts)")
    columns = [column[1] for column in c.fetchall()]
    
    # Добавляем новые колонки, если их нет
    if 'steam_guard_enabled' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN steam_guard_enabled INTEGER DEFAULT 1")
    if 'order_id' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN order_id TEXT")
    if 'bonus_given' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN bonus_given INTEGER DEFAULT 0")
    if 'friend_mode' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN friend_mode INTEGER DEFAULT 0")
    if 'warned_10min' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN warned_10min INTEGER DEFAULT 0")
    if 'rented_by' not in columns:
        c.execute("ALTER TABLE accounts ADD COLUMN rented_by INTEGER")
    
    conn.commit()
    conn.close()

def set_friend_mode(tg_user_id):
    """Активирует режим friend для пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    current_time = int(time.time())
    
    logger.info(f"[FRIEND] Активация режима friend для пользователя {tg_user_id}")
    
    # Удаляем старые настройки
    c.execute("DELETE FROM friend_mode_settings WHERE tg_user_id=?", (tg_user_id,))
    
    # Добавляем новые с явным указанием is_active=1
    c.execute("INSERT INTO friend_mode_settings (tg_user_id, activated_at, is_active) VALUES (?, ?, 1)",
              (tg_user_id, current_time))
    
    conn.commit()
    conn.close()
    logger.info(f"[FRIEND] Режим friend активирован для пользователя {tg_user_id}")

def is_friend_mode_active(tg_user_id):
    """Проверяет активен ли режим friend для пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    current_time = int(time.time())
    
    # Проверяем наличие активных настроек
    c.execute("""
        SELECT 1 FROM friend_mode_settings 
        WHERE tg_user_id=? AND is_active=1 
        AND activated_at > ? - 600
    """, (tg_user_id, current_time))
    
    result = c.fetchone() is not None
    
    # Если настройки устарели, деактивируем их
    if not result:
        c.execute("""
            UPDATE friend_mode_settings 
            SET is_active=0 
            WHERE tg_user_id=? AND activated_at <= ? - 600
        """, (tg_user_id, current_time))
        conn.commit()
        if c.rowcount > 0:
            logger.info(f"[FRIEND] Режим friend деактивирован для пользователя {tg_user_id} (истекло время)")
    
    conn.close()
    logger.info(f"[FRIEND] Проверка режима friend для пользователя {tg_user_id}: {'активен' if result else 'неактивен'}")
    return result

def clear_friend_mode(tg_user_id):
    """Очищает настройки режима friend для пользователя"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM friend_mode_settings WHERE tg_user_id=?", (tg_user_id,))
    conn.commit()
    conn.close()
    logger.info(f"[FRIEND] Настройки режима friend очищены для пользователя {tg_user_id}")

def cleanup_expired_friend_modes():
    """Очищает устаревшие настройки режима friend"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    current_time = int(time.time())
    
    # Сначала деактивируем устаревшие настройки
    c.execute("""
        UPDATE friend_mode_settings 
        SET is_active=0 
        WHERE activated_at < ? - 600
    """, (current_time,))
    deactivated = c.rowcount
    
    # Затем удаляем записи старше 1 часа
    c.execute("""
        DELETE FROM friend_mode_settings 
        WHERE activated_at < ? - 3600
    """, (current_time,))
    deleted = c.rowcount
    
    conn.commit()
    conn.close()
    logger.info(f"[FRIEND] Очистка устаревших настроек: деактивировано {deactivated}, удалено {deleted} записей")

def restore_rental_timers():
    logger.info("[RESTORE] Восстановление таймеров аренды...")
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, tg_user_id, rented_until FROM accounts WHERE status='rented'")
    active_rentals = c.fetchall()
    conn.close()
    current_time = time.time()
    for acc_id, tg_user_id, rented_until in active_rentals:
        if rented_until and float(rented_until) > current_time:
            remaining_time = float(rented_until) - current_time
            logger.info(f"[RESTORE] Восстанавливаем таймер аренды для аккаунта {acc_id}, осталось {remaining_time} секунд")
            try:
                from steam.steam_account_rental_utils import auto_end_rent, mark_account_free
                def notify_callback(acc_id, tg_user_id):
                    logger.info(f"[RESTORE] Автоосвобождение аккаунта {acc_id}")
                    mark_account_free(acc_id)
                    
                    # Получаем order_id для отправки сообщения FunPay клиенту
                    order_id = None
                    try:
                        conn_inner = sqlite3.connect(DB_PATH)
                        c_inner = conn_inner.cursor()
                        c_inner.execute("SELECT order_id FROM accounts WHERE id=?", (acc_id,))
                        row_inner = c_inner.fetchone()
                        if row_inner and row_inner[0]:
                            order_id = row_inner[0]
                        conn_inner.close()
                    except Exception as e_inner:
                        logger.error(f"[RESTORE] Ошибка при получении order_id для аккаунта {acc_id}: {e_inner}")

                    if tg_user_id and order_id and not str(order_id).startswith('TG-'): # Отправляем только FunPay ордерам, не Telegram
                        try:
                            funpay = FunPayListener()
                            order_data = {'chat_id': tg_user_id, 'order_id': order_id}
                            send_order_completed_message(order_data, 
                                lambda chat_id_arg, text: funpay.funpay_send_message_wrapper(chat_id_arg, text))
                            logger.info(f"[RESTORE] Сообщение об окончании аренды отправлено клиенту FunPay {tg_user_id} для заказа {order_id}")
                        except Exception as e_funpay:
                            logger.error(f"[RESTORE] Ошибка при отправке сообщения клиенту FunPay {tg_user_id} об окончании аренды: {e_funpay}")

                auto_end_rent(acc_id, tg_user_id, int(remaining_time), notify_callback=notify_callback)
            except Exception as e:
                logger.error(f"[RESTORE] Ошибка при восстановлении таймера для аккаунта {acc_id}: {e}")
        else:
            try:
                from steam.steam_account_rental_utils import mark_account_free
                mark_account_free(acc_id)
                logger.info(f"[RESTORE] Аккаунт {acc_id} освобождён (время аренды истекло)")
            except Exception as e:
                logger.error(f"[RESTORE] Ошибка при освобождении аккаунта {acc_id}: {e}") 
# next update