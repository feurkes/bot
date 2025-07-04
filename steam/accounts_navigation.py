# --- ВЫБОР ИГРЫ И НАВИГАЦИЯ ПО АККАУНТАМ ---
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import sqlite3
import os

# Путь к базе данных
DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'storage', 'plugins')
DB_PATH = os.path.join(DB_DIR, 'steam_rental.db')

# --- ВЫБОР ИГРЫ И НАВИГАЦИЯ ПО АККАУНТАМ ---
def games_menu():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT DISTINCT game_name FROM accounts")
    games = [r[0] for r in c.fetchall()]
    conn.close()
    markup = InlineKeyboardMarkup()
    for game in games:
        markup.add(InlineKeyboardButton(game, callback_data=f"select_game:{game}"))
    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_menu"))
    return markup

def show_accounts_page(bot, call, game, idx):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, login, password, status, steam_guard_enabled FROM accounts WHERE game_name=? ORDER BY id", (game,))
        accounts = c.fetchall()
        conn.close()
        
        total = len(accounts)
        if total == 0:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="list_accs"))
            bot.edit_message_text("Нет аккаунтов для выбранной игры.", call.message.chat.id, call.message.id, reply_markup=markup)
            return
            
        idx = max(0, min(idx, total-1))
        acc_id, login, password, status, steam_guard_enabled = accounts[idx]
        
        text = f"<b>Игра:</b> <code>{game}</code>\n<b>Логин:</b> <code>{login}</code>\n<b>Пароль:</b> <code>{password}</code>\n<b>Статус:</b> <b>{'🟢 Свободен' if status=='free' else '🔴 В аренде'}</b>"

        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("🧪 Тест", callback_data=f"test:{acc_id}"))
        kb.add(InlineKeyboardButton("⛔️ Фулл выход", callback_data=f"logout:{acc_id}"))
        kb.add(InlineKeyboardButton("📝 Сменить данные", callback_data=f"chgdata:{acc_id}"))
        kb.add(InlineKeyboardButton(
            "🟢 Искать код для клиента" if steam_guard_enabled else "🔴 Не искать код для клиента",
            callback_data=f"toggle_guard:{acc_id}:{game}:{idx}"
        ))

        if status == "free":
            kb.add(InlineKeyboardButton("🟢 Арендовать", callback_data=f"rent:{acc_id}"))
            kb.add(InlineKeyboardButton("🗑 Удалить", callback_data=f"del:{acc_id}"))
        else:
            kb.add(InlineKeyboardButton("⏹ Завершить аренду", callback_data=f"return:{acc_id}"))
            kb.add(InlineKeyboardButton("📨 Steam Guard", callback_data=f"guard:{acc_id}"))

        # --- СТРОКА НАВИГАЦИИ (стрелки) ---
        nav = []
        if idx > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"acc_nav:{game}:{idx-1}"))
        if idx < total-1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"acc_nav:{game}:{idx+1}"))
        if nav:
            kb.row(*nav)

        # --- КНОПКА "К играм" ВСЕГДА В САМОМ НИЗУ ---
        kb.add(InlineKeyboardButton("⬅️ К играм", callback_data="list_accs"))

        bot.edit_message_text(text, call.message.chat.id, call.message.id, parse_mode="HTML", reply_markup=kb)
        
    except Exception as e:
        logging.error(f"Ошибка при отображении аккаунта: {e}")
        bot.send_message(call.message.chat.id, f"❌ Произошла ошибка при отображении аккаунта: {str(e)}")

# --- ИНИЦИАЛИЗАЦИЯ ОБРАБОТЧИКОВ ---
def init_accounts_navigation(bot):
    # Регистрируем только обработчик acc_nav, который не определен в основном файле
    def cb_acc_nav(call):
        bot.answer_callback_query(call.id)
        
        # Правильный парсинг callback_data: acc_nav:<game_name>:<index>
        # Название игры может содержать двоеточия. Индекс - после последнего двоеточия.
        data_parts = call.data.split(":")
        # prefix = data_parts[0] # Можно не использовать, если уверен в формате
        idx_str = data_parts[-1] # Последняя часть - это индекс
        game = ":".join(data_parts[1:-1]) # Все части между первой и последней - это название игры
        
        try:
            idx = int(idx_str)
            from standalone_steam_rental_bot import show_accounts_page as show_page_main
            show_page_main(call, game, idx)
        except ValueError as e:
            logging.error(f"Ошибка парсинга индекса страницы из callback: {call.data}, ошибка: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при навигации по аккаунтам: Неверный формат страницы.")
        except Exception as e:
            logging.error(f"Неожиданная ошибка в cb_acc_nav: {e}")
            bot.send_message(call.message.chat.id, f"Произошла ошибка при навигации по аккаунтам.")
    bot.callback_query_handler(func=lambda c: c.data.startswith("acc_nav:"))(cb_acc_nav)
