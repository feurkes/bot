"""Utility functions for Telegram bot handlers"""

import sqlite3
import os
import html
from tg_utils.db import DB_PATH
from tg_utils.logger import logger
from tg_utils.state import user_states, user_acc_data
from tg_utils.keyboards import main_menu
from game_name_mapper import mapper


def parse_imap_host_port(imap_str):
    """Parse IMAP host and port from string"""
    if ':' in imap_str:
        host, port = imap_str.rsplit(':', 1)
        try:
            port = int(port)
        except ValueError:
            port = None
        return host.strip(), port
    return imap_str.strip(), None


def finalize_add_account(message, user_id, bot):
    """Finalize adding account to database"""
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


def send_steam_success_log(page, chat_id, bot, login=None, password=None):
    """Send Steam success log with screenshot"""
    try:
        if login and password:
            escaped_login = html.escape(login)
            escaped_password = html.escape(password)
            bot.send_message(chat_id, f"✅ <b>Успешный вход в Steam!</b>\n\nЛогин: <code>{escaped_login}</code>\nПароль: <code>{escaped_password}</code>", parse_mode="HTML")
        else:
            bot.send_message(chat_id, "✅ <b>Успешный вход в Steam!</b>", parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка при отправке лога: {e}")


def handle_custom_rent_time(message, bot, user_states):
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
            
            # Execute rent with custom time - this will be implemented in account_management
            # from .account_management import execute_rent
            # class MockCall:
            #     def __init__(self, message):
            #         self.message = message
            #         
            # mock_call = MockCall(message)
            # execute_rent(mock_call, acc_id, hours)
            
        except ValueError:
            bot.send_message(message.chat.id, "❌ Введите корректное число часов")
            
    except Exception as e:
        logger.error(f"Error handling custom rent time: {e}")