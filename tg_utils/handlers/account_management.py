"""Account management handlers for Telegram bot"""

import sqlite3
import html
import threading
import time
from datetime import datetime, timedelta
from telebot import types
from .base import BaseHandler
from tg_utils.db import DB_PATH
from tg_utils.state import user_states, user_acc_data
from tg_utils.keyboards import main_menu, game_selection_kb, get_game_emoji
from tg_utils.helpers import safe_edit_message_text
from .utils import finalize_add_account
from steam.steam_account_rental_utils import mark_account_rented, mark_account_free, send_account_to_buyer
from tg_utils.logger import logger


def init_account_management_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """Initialize account management handlers"""
    
    # Create handler instance
    handler = BaseHandler(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    bot = bot_instance
    auth_required = auth_required_decorator or (lambda func: func)

    # --- ДОБАВЛЕНИЕ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data == "add_acc")
    @auth_required
    def cb_add_acc(call):
        handler.answer_callback_query(call)
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
            "📧 <b>Шаг 5/6: Email аккаунта</b>\n\n"
            "📮 Введите email, привязанный к Steam аккаунту\n"
            "🔑 Он нужен для автоматического получения кодов Steam Guard\n\n"
            "✅ <b>Поддерживаемые провайдеры:</b>\n"
            "• Gmail (gmail.com)\n"
            "• Yandex (yandex.ru, ya.ru)\n"
            "• Mail.ru\n"
            "• Outlook/Hotmail\n"
            "• Rambler\n\n"
            "📝 <b>Пример:</b> myemail@gmail.com"
        )
        bot.send_message(message.chat.id, mail_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail")
    @auth_required
    def add_mail_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["email_login"] = message.text.strip()
        user_states[user_id] = "add_mail_pw"
        
        mail_pw_text = (
            "🔐 <b>Шаг 6/6: Пароль от почты</b>\n\n"
            "🔑 Введите пароль от почтового ящика\n"
            "⚠️ <b>Для Gmail/Outlook:</b> используйте пароль приложения, а не основной пароль\n\n"
            "🛡️ Пароль будет зашифрован и храниться в безопасности\n\n"
            "💡 <b>Примечание:</b> Если используете двухфакторную аутентификацию, создайте пароль для приложений в настройках почты"
        )
        bot.send_message(message.chat.id, mail_pw_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail_pw")
    @auth_required
    def add_mail_pw_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["email_password"] = message.text.strip()
        
        # Определяем IMAP настройки автоматически
        email = user_acc_data[user_id]["email_login"]
        domain = email.split('@')[-1].lower()
        
        imap_presets = {
            'gmail.com': 'imap.gmail.com:993',
            'yandex.ru': 'imap.yandex.ru:993',
            'ya.ru': 'imap.yandex.ru:993',
            'mail.ru': 'imap.mail.ru:993',
            'outlook.com': 'imap-mail.outlook.com:993',
            'hotmail.com': 'imap-mail.outlook.com:993',
            'rambler.ru': 'imap.rambler.ru:993'
        }
        
        if domain in imap_presets:
            user_acc_data[user_id]["imap_host"] = imap_presets[domain]
            finalize_add_account(message, user_id, bot)
        else:
            user_states[user_id] = "add_mail_imap"
            imap_text = (
                f"⚙️ <b>Настройка IMAP для {domain}</b>\n\n"
                f"🔧 Домен {domain} не найден в автоматических настройках\n"
                f"📝 Введите IMAP сервер в формате: host:port\n\n"
                f"📋 <b>Примеры:</b>\n"
                f"• imap.example.com:993\n"
                f"• mail.example.com:143\n\n"
                f"💡 <b>Совет:</b> Обычно используется порт 993 для SSL или 143 для обычного соединения"
            )
            bot.send_message(message.chat.id, imap_text, parse_mode="HTML")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "add_mail_imap")
    @auth_required
    def add_mail_imap_step(message):
        user_id = message.from_user.id
        user_acc_data[user_id]["imap_host"] = message.text.strip()
        finalize_add_account(message, user_id, bot)

    # --- СПИСОК АККАУНТОВ ---
    @bot.callback_query_handler(func=lambda c: c.data == "list_accs")
    @auth_required
    def cb_list_accs(call):
        handler.answer_callback_query(call)
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
        handler.log_debug(f"[SELECT_GAME] Callback received: {call.data}")
        handler.answer_callback_query(call)
        try:
            game = call.data.split(":", 1)[1]
            handler.log_debug(f"[SELECT_GAME] Game selected: {game}")
            show_accounts_page(call.message, game, 0)
        except Exception as e:
            handler.log_error(f"[SELECT_GAME] Error handling select_game callback {call.data}", e)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
            markup.add(types.InlineKeyboardButton("🔄 В главное меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                                   "Произошла ошибка при выборе игры.", reply_markup=markup)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("page:"))
    @auth_required
    def cb_page_accs(call):
        handler.answer_callback_query(call)
        try:
            parts = call.data.split(":")
            game = parts[1]
            page_num = int(parts[2])
            start_index = page_num * 10
            show_accounts_page(call.message, game, start_index)
        except Exception as e:
            handler.log_error(f"Error handling page callback {call.data}", e)

    def show_accounts_page(message, game, start_index):
        """Show accounts page with pagination"""
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM accounts WHERE game_name=?", (game,))
            total_accs = c.fetchone()[0]
            
            c.execute("SELECT id, login, status FROM accounts WHERE game_name=? ORDER BY id LIMIT 10 OFFSET ?", 
                     (game, start_index))
            accounts = c.fetchall()
            conn.close()

            if not accounts:
                text = f"❌ Нет аккаунтов для игры <b>{html.escape(game)}</b>"
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))
                safe_edit_message_text(bot, message.chat.id, message.message_id, text, 
                                       reply_markup=markup, parse_mode="HTML")
                return

            # Формируем текст
            text = f"🎮 <b>Аккаунты для игры: {html.escape(game)}</b>\n\n"
            
            for acc_id, login, status in accounts:
                status_emoji = "🟢" if status == "free" else "🔴"
                status_text = "Свободен" if status == "free" else "В аренде"
                text += f"{status_emoji} <b>ID:</b> {acc_id} | <b>Логин:</b> <code>{html.escape(login)}</code> | <b>Статус:</b> {status_text}\n"

            # Пагинация
            total_pages = (total_accs + 9) // 10
            current_page = start_index // 10
            text += f"\n📄 Страница {current_page + 1} из {total_pages}"

            # Создаем клавиатуру
            markup = types.InlineKeyboardMarkup(row_width=1)
            
            # Кнопки аккаунтов
            for acc_id, login, status in accounts:
                status_emoji = "🟢" if status == "free" else "🔴"
                markup.add(types.InlineKeyboardButton(
                    f"{status_emoji} {login}", 
                    callback_data=f"info:{acc_id}"
                ))

            # Кнопки навигации
            nav_row = []
            if current_page > 0:
                nav_row.append(types.InlineKeyboardButton("◀️ Пред", callback_data=f"page:{game}:{current_page-1}"))
            if current_page < total_pages - 1:
                nav_row.append(types.InlineKeyboardButton("След ▶️", callback_data=f"page:{game}:{current_page+1}"))
            
            if nav_row:
                markup.row(*nav_row)

            markup.add(types.InlineKeyboardButton("⬅️ Назад к играм", callback_data="list_accs"))

            safe_edit_message_text(bot, message.chat.id, message.message_id, text, 
                                   reply_markup=markup, parse_mode="HTML")

        except Exception as e:
            handler.log_error(f"Error in show_accounts_page for game {game}", e)

    # --- ИНФОРМАЦИЯ ОБ АККАУНТЕ ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("info:"))
    @auth_required
    def cb_info(call):
        handler.answer_callback_query(call)
        try:
            acc_id = call.data.split(":", 1)[1]
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT id, login, password, game_name, status, steam_guard_enabled, rented_until, email_login, email_password FROM accounts WHERE id=?", (acc_id,))
            row = c.fetchone()
            conn.close()

            if not row:
                handler.answer_callback_query(call, "❌ Аккаунт не найден")
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

            # Добавляем информацию об аренде
            if status == "rented" and rented_until_timestamp:
                try:
                    rented_until_dt = datetime.fromtimestamp(rented_until_timestamp)
                    now = datetime.now()
                    remaining_time = rented_until_dt - now

                    end_time_str = rented_until_dt.strftime('%H:%M:%S %d.%m.%Y')

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

                    text += f"\nАрендован до (локальное время сервера): {html.escape(end_time_str)}"
                    text += f"\nОсталось: {html.escape(remaining_str)}"

                except Exception as e:
                    handler.log_error("Ошибка при расчете времени аренды", e)
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

            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, 
                                   reply_markup=markup, parse_mode="HTML")

        except Exception as e:
            handler.log_error(f"[CB_INFO] Ошибка при отображении информации об аккаунте {call.data}", e)
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("⬅️ В меню", callback_data="back_to_menu"))
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                                   "Произошла ошибка при получении информации об аккаунте.", 
                                   reply_markup=markup, parse_mode="HTML")

    # --- АРЕНДА АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent:"))
    @auth_required
    def cb_rent(call):
        handler.answer_callback_query(call)
        try:
            acc_id = call.data.split(":")[1]
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
            
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, 
                                   reply_markup=markup, parse_mode="HTML")

        except Exception as e:
            handler.log_error("Ошибка в cb_rent", e)
            try:
                bot.edit_message_text("Произошла ошибка при выборе времени аренды.", 
                                     call.message.chat.id, call.message.message_id, reply_markup=main_menu())
            except Exception:
                bot.send_message(call.message.chat.id, "Произошла ошибка при выборе времени аренды.", 
                                reply_markup=main_menu())

    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent_time:"))
    @auth_required
    def cb_rent_time(call):
        handler.answer_callback_query(call)
        try:
            parts = call.data.split(":")
            acc_id = parts[1]
            hours = int(parts[2])
            
            execute_rent(call, acc_id, hours)

        except Exception as e:
            handler.log_error("Ошибка в cb_rent_time", e)

    @bot.callback_query_handler(func=lambda c: c.data.startswith("rent_custom:"))
    @auth_required
    def cb_rent_custom(call):
        handler.answer_callback_query(call)
        try:
            acc_id = call.data.split(":")[1]
            user_id = call.from_user.id
            
            # Сохраняем ID аккаунта в состоянии пользователя
            user_states[user_id] = {
                'state': 'awaiting_custom_rent_time',
                'acc_id': acc_id
            }
            
            text = (
                "⏰ <b>Кастомное время аренды</b>\n\n"
                "🕐 Введите время аренды в часах\n\n"
                "📝 <b>Примеры:</b>\n"
                "• <code>0.5</code> - 30 минут\n"
                "• <code>2</code> - 2 часа\n"
                "• <code>12.5</code> - 12 часов 30 минут\n"
                "• <code>168</code> - 7 дней (максимум)\n\n"
                "⚠️ <b>Ограничения:</b> от 0.1 до 168 часов"
            )
            
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("◀️ Назад", callback_data=f"rent:{acc_id}"))
            
            safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, 
                                   reply_markup=markup, parse_mode="HTML")
            
        except Exception as e:
            handler.log_error("Ошибка в cb_rent_custom", e)

    @bot.message_handler(func=lambda message: user_states.get(message.from_user.id, {}).get('state') == 'awaiting_custom_rent_time')
    @auth_required
    def handle_custom_rent_time(message):
        """Handle custom rental time input"""
        try:
            user_id = message.from_user.id
            
            # Get the account ID from user state
            if user_id not in user_states or not isinstance(user_states[user_id], dict):
                return
                
            acc_id = user_states[user_id].get('acc_id')
            if not acc_id:
                return
                
            # Parse the time input
            time_text = message.text.strip()
            
            try:
                hours = float(time_text)
                if hours <= 0 or hours > 168:  # Max 1 week
                    bot.send_message(message.chat.id, "❌ Время аренды должно быть от 0.1 до 168 часов")
                    return
                    
                # Clear state
                user_states[user_id] = None
                
                # Execute rent with custom time
                class MockCall:
                    def __init__(self, message):
                        self.message = message
                        self.from_user = message.from_user
                        
                mock_call = MockCall(message)
                execute_rent(mock_call, acc_id, hours)
                
            except ValueError:
                bot.send_message(message.chat.id, "❌ Введите корректное число часов")
                
        except Exception as e:
            handler.log_error("Error handling custom rent time", e)

    def execute_rent(call, acc_id, hours):
        """Execute account rental"""
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
                handler.log_error("Ошибка при маркировке аккаунта как арендованного", e)
                bot.send_message(call.message.chat.id, "❌ Ошибка при аренде аккаунта.")
                return

            # Отправляем данные аккаунта пользователю
            order = {'chat_id': call.message.chat.id, 'buyer': call.from_user.id, 'description': acc[3]}
            
            try:
                pass  # если нужно, оставим вызов для FunPay отдельно
            except Exception as e:
                handler.log_error("Ошибка при отправке данных аккаунта", e)

            # Успешная аренда
            rent_text = (
                f"✅ <b>Аккаунт успешно арендован!</b>\n\n"
                f"🎮 <b>Логин:</b> <code>{html.escape(acc[1])}</code>\n"
                f"🔐 <b>Пароль:</b> <code>{html.escape(acc[2])}</code>\n"
                f"🎯 <b>Игра:</b> {html.escape(acc[3])}\n"
                f"⏰ <b>Время аренды:</b> {hours} ч.\n"
                f"📅 <b>До:</b> {rented_until.strftime('%H:%M:%S %d.%m.%Y')}\n\n"
                f"🔥 Приятной игры!"
            )

            msg = bot.send_message(call.message.chat.id, rent_text, parse_mode="HTML")

            # Удаляем сообщение через 10 секунд
            def delete_rent_message():
                time.sleep(10)
                try:
                    bot.delete_message(call.message.chat.id, msg.message_id)
                except Exception as e:
                    handler.log_error("Ошибка при удалении сообщения об аренде", e)

            threading.Thread(target=delete_rent_message, daemon=True).start()

        except Exception as e:
            handler.log_error("Ошибка в execute_rent", e)
            bot.send_message(call.message.chat.id, "❌ Произошла ошибка при аренде аккаунта.")

    # --- ВОЗВРАТ АККАУНТА ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("return:"))
    @auth_required
    def cb_return(call):
        handler.answer_callback_query(call)
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
                bot.send_message(call.message.chat.id, "❌ Аккаунт не найден.")
                return
            if row[0] != "rented":
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
                        handler.log_error("Ошибка при отправке сообщения клиенту FunPay", e)
                # Сообщение админу в Telegram
                msg = bot.send_message(call.message.chat.id, admin_msg)

                def delete_admin_msg():
                        time.sleep(5)
                        try:
                            bot.delete_message(call.message.chat.id, msg.message_id)
                        except Exception as e:
                            handler.log_error("Ошибка при удалении сообщения о завершении аренды", e)
                
                threading.Thread(target=delete_admin_msg, daemon=True).start()
                
            except Exception as e:
                handler.log_error("Ошибка при завершении аренды", e)
                bot.send_message(call.message.chat.id, "❌ Ошибка при завершении аренды.")
                
        except Exception as e:
            handler.log_error("Ошибка в cb_return", e)

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
            handler.answer_callback_query(call, "❌ Аккаунт не найден")
            return
        
        login = row[0]
        c.execute("DELETE FROM accounts WHERE id=?", (acc_id,))
        conn.commit()
        conn.close()
        
        text = f"✅ Аккаунт <b>{login}</b> удален"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("◀️ В меню", callback_data="back_to_menu"))
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, text, 
                               reply_markup=markup, parse_mode="HTML")