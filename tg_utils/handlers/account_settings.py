"""Account settings handlers for Telegram bot"""

import sqlite3
import threading
import time
from telebot import types
from .base import BaseHandler
from tg_utils.db import DB_PATH
from tg_utils.state import user_states, user_acc_data
from tg_utils.keyboards import main_menu
from tg_utils.logger import logger
from .utils import parse_imap_host_port


def init_account_settings_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """Initialize account settings handlers"""
    
    # Create handler instance
    handler = BaseHandler(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    bot = bot_instance
    auth_required = auth_required_decorator or (lambda func: func)

    # --- ПЕРЕКЛЮЧЕНИЕ STEAM GUARD ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("toggle_guard:"))
    @auth_required
    def cb_toggle_guard(call):
        handler.log_debug(f"[TOGGLE] cb_toggle_guard вызван с данными: {call.data}")
        handler.answer_callback_query(call, "Обработка...", False)
        try:
            # Парсим данные из callback_data
            parts = call.data.split(":", 2)  # Разделяем только на 3 части
            if len(parts) != 3:
                handler.log_error(f"[TOGGLE] Неверный формат callback_data: {call.data}")
                return
                
            acc_id = parts[1]
            game_name = parts[2]  # Оставшаяся часть - это название игры
            
            handler.log_debug(f"[TOGGLE] Переключение Steam Guard для аккаунта {acc_id}")
            
            # Подключаемся к БД и получаем текущее значение
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc_id,))
            row = c.fetchone()
            
            if not row:
                handler.log_error(f"[TOGGLE] Аккаунт {acc_id} не найден в БД")
                conn.close()
                return
                
            current_state = row[0]
            handler.log_debug(f"[TOGGLE] Текущее значение из БД: {current_state} (тип: {type(current_state)})")
            
            # Приводим к int и проверяем корректность
            try:
                current_state = int(current_state) if current_state is not None else 1
            except (ValueError, TypeError):
                handler.log_warning(f"[TOGGLE] Некорректное значение в БД: {row[0]}, используем значение по умолчанию 1")
                current_state = 1
                
            # Инвертируем состояние
            new_state = 0 if current_state else 1
            
            handler.log_debug(f"[TOGGLE] Текущее состояние: {current_state}, новое состояние: {new_state}")
            
            # Обновляем в БД
            c.execute("UPDATE accounts SET steam_guard_enabled = ? WHERE id=?", (new_state, acc_id))
            conn.commit()
            
            # Проверяем, что обновление прошло успешно
            c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc_id,))
            updated_row = c.fetchone()
            
            if updated_row:
                try:
                    updated_state = int(updated_row[0]) if updated_row[0] is not None else 1
                except (ValueError, TypeError):
                    handler.log_error(f"[TOGGLE] Некорректное значение после обновления: {updated_row[0]}")
                    updated_state = new_state
            else:
                handler.log_error("[TOGGLE] Не удалось получить обновленное значение")
                updated_state = new_state
                
            conn.close()
            
            if updated_state == new_state:
                handler.log_debug("[TOGGLE] Состояние успешно обновлено в БД")
                # Отвечаем на callback
                state_text = "включено" if new_state else "выключено"
                handler.answer_callback_query(call, f"Steam Guard {state_text}")
                
                # Перенаправляем обратно к информации об аккаунте
                try:
                    # Имитируем новый callback для cb_info
                    class MockCall:
                        def __init__(self, message, data):
                            self.message = message
                            self.data = data
                            self.id = call.id
                            
                    mock_call = MockCall(call.message, f"info:{acc_id}")
                    
                    # Вызываем cb_info напрямую (нужно импортировать из account_management)
                    from .account_management import cb_info
                    # Мы не можем вызвать cb_info здесь, так как это создаст циклическую зависимость
                    # Вместо этого, просто обновим текущее сообщение
                    
                    # Получаем обновленную информацию об аккаунте
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute("SELECT steam_guard_enabled FROM accounts WHERE id=?", (acc_id,))
                    row = c.fetchone()
                    conn.close()
                    
                    if row:
                        steam_guard_enabled = int(row[0]) if row[0] is not None else 1
                        guard_button_text = "🟢 Искать код" if steam_guard_enabled else "🔴 Не искать код"
                        
                        # Просто отправляем подтверждение
                        bot.send_message(call.message.chat.id, 
                                       f"✅ Steam Guard {state_text} для аккаунта {acc_id}")
                    
                except Exception as e:
                    handler.log_error(f"[TOGGLE] Ошибка при обновлении интерфейса", e)
                    bot.send_message(call.message.chat.id, f"✅ Steam Guard {state_text}")
            else:
                handler.log_error(f"[TOGGLE] Ошибка: состояние в БД ({updated_state}) не соответствует ожидаемому ({new_state})")
                handler.answer_callback_query(call, "❌ Ошибка при обновлении", True)
                
        except Exception as e:
            handler.log_error(f"[TOGGLE] Общая ошибка в cb_toggle_guard", e)
            handler.answer_callback_query(call, "❌ Произошла ошибка", True)

    # --- НАСТРОЙКИ ПОЧТЫ ---
    @bot.callback_query_handler(func=lambda c: c.data.startswith("mail:"))
    @auth_required
    def cb_mail(call):
        try:
            acc_id = call.data.split(":")[1]
            user_id = call.from_user.id

            if user_id not in user_acc_data:
                user_acc_data[user_id] = {}
            user_acc_data[user_id]['chg_acc_id'] = acc_id
            user_states[user_id] = "change_mail_login"

            handler.answer_callback_query(call, "✏️ Введите новый логин почты:")
            bot.delete_message(call.message.chat.id, call.message.message_id)

        except Exception as e:
            handler.log_error(f"Error handling mail callback {call.data}", e)
            handler.answer_callback_query(call, "❌ Произошла ошибка при настройке почты")

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_login")
    @auth_required
    def change_mail_login_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_mail_login = message.text.strip()

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET email_login=? WHERE id=?", (new_mail_login, acc_id))
            conn.commit()
            conn.close()

            user_acc_data[user_id]['new_mail_login'] = new_mail_login
            user_states[user_id] = "change_mail_password"
            bot.send_message(message.chat.id, "✏️ Введите новый пароль почты:")

        except Exception as e:
            handler.log_error(f"Error changing mail login for account {acc_id}", e)
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене логина почты.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_password")
    @auth_required
    def change_mail_password_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_mail_password = message.text.strip()

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET email_password=? WHERE id=?", (new_mail_password, acc_id))
            conn.commit()
            conn.close()

            user_acc_data[user_id]['new_mail_password'] = new_mail_password
            user_states[user_id] = "change_mail_imap"
            
            # Определяем IMAP настройки автоматически
            email = user_acc_data[user_id]['new_mail_login']
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
                # Автоматически устанавливаем IMAP
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("UPDATE accounts SET imap_host=? WHERE id=?", (imap_presets[domain], acc_id))
                conn.commit()
                conn.close()
                
                bot.send_message(message.chat.id, 
                    f"✅ Настройки почты успешно обновлены!\n\n"
                    f"📧 Email: {email}\n"
                    f"🌐 IMAP: {imap_presets[domain]}", 
                    reply_markup=main_menu())
                
                user_states.pop(user_id, None)
                user_acc_data.pop(user_id, None)
            else:
                bot.send_message(message.chat.id, 
                    f"⚙️ Введите IMAP сервер для {domain} в формате host:port\n"
                    f"Например: imap.{domain}:993")

        except Exception as e:
            handler.log_error(f"Error changing mail password for account {acc_id}", e)
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене пароля почты.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    @bot.message_handler(func=lambda m: user_states.get(m.from_user.id) == "change_mail_imap")
    @auth_required
    def change_mail_imap_step(message):
        user_id = message.from_user.id
        if user_id not in user_acc_data or 'chg_acc_id' not in user_acc_data[user_id]:
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Начните заново.", reply_markup=main_menu())
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)
            return

        acc_id = user_acc_data[user_id]['chg_acc_id']
        new_imap_host = message.text.strip()

        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("UPDATE accounts SET imap_host=? WHERE id=?", (new_imap_host, acc_id))
            conn.commit()
            conn.close()

            email = user_acc_data[user_id]['new_mail_login']
            bot.send_message(message.chat.id, 
                f"✅ Настройки почты успешно обновлены!\n\n"
                f"📧 Email: {email}\n"
                f"🌐 IMAP: {new_imap_host}", 
                reply_markup=main_menu())

            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

        except Exception as e:
            handler.log_error(f"Error changing IMAP for account {acc_id}", e)
            bot.send_message(message.chat.id, "❌ Произошла ошибка при смене IMAP.")
            user_states.pop(user_id, None)
            user_acc_data.pop(user_id, None)

    # --- НАСТРОЙКИ ВХОДА ---
    @bot.callback_query_handler(func=lambda call: call.data.startswith('login_settings:'))
    def cb_login_settings(call=None, chat_id=None, message_id=None, account_id=None):
        try:
            # Определяем chat_id, message_id и account_id в зависимости от способа вызова
            if call:
                handler.log_info(f"[CALLBACK] Received callback: {call.data}")
                current_chat_id = call.message.chat.id
                current_message_id = call.message.message_id
                account_id = call.data.split(':')[1]  # id теперь всегда строка
                # Мы отвечаем на callback здесь, если функция вызвана через callback
                handler.answer_callback_query(call, "Настройки входа")
            elif chat_id is not None and message_id is not None and account_id is not None:
                current_chat_id = chat_id
                current_message_id = message_id
                # account_id уже передан
                handler.log_info(f"[CALLBACK] Called cb_login_settings programmatically for account {account_id} at chat_id {chat_id}, message_id {message_id}")
                # Здесь мы не отвечаем на callback, так как его обработал вызвавший код (например, cb_cancel_input)
            else:
                handler.log_error("[CALLBACK] cb_login_settings called with insufficient arguments.")
                # Недостаточно аргументов для работы, нет chat_id для отправки сообщения об ошибке
                return

            # Здесь нужно получить game_name для кнопки Назад
            # В идеале, эти данные должны быть частью callback_data или извлечены из БД по account_id
            # Для простоты сейчас извлеку из БД
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('SELECT game_name FROM accounts WHERE id = ?', (account_id,))
            result = cursor.fetchone()
            conn.close()

            game_name = result[0] if result else "Unknown Game"

            keyboard = types.InlineKeyboardMarkup()

            # Кнопки для изменения данных входа
            # Callback data будет содержать account_id и тип поля для изменения
            keyboard.add(types.InlineKeyboardButton("🔑 Логин Steam", callback_data=f"change_field:{account_id}:login"))
            keyboard.add(types.InlineKeyboardButton("🔐 Пароль Steam", callback_data=f"change_field:{account_id}:password"))
            keyboard.add(types.InlineKeyboardButton("📧 Почта", callback_data=f"change_field:{account_id}:email_login"))
            keyboard.add(types.InlineKeyboardButton("🔒 Пароль от почты", callback_data=f"change_field:{account_id}:email_password"))
            keyboard.add(types.InlineKeyboardButton("🌐 IMAP сервер", callback_data=f"change_field:{account_id}:imap_host"))

            # Кнопка Назад в главное меню настроек аккаунта
            # Важно: здесь callback_data должен вести обратно к info:{account_id}
            back_to_info_callback = f"info:{account_id}"
            keyboard.add(types.InlineKeyboardButton("◀️ Назад", callback_data=back_to_info_callback))

            text = f"⚙️ Настройки входа для аккаунта #{account_id}"

            try:
                # Пробуем отредактировать существующее сообщение
                bot.edit_message_text(
                    chat_id=current_chat_id,
                    message_id=current_message_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )
            except Exception as edit_error:
                handler.log_error(f"[CALLBACK] Ошибка при редактировании сообщения в cb_login_settings", edit_error)
                # Если редактирование не удалось, отправляем новое сообщение
                bot.send_message(
                    chat_id=current_chat_id,
                    text=text,
                    reply_markup=keyboard,
                    parse_mode="HTML"
                )

        except Exception as e:
            handler.log_error(f"[CALLBACK] Ошибка в cb_login_settings", e)
            # Если есть call, отправляем сообщение об ошибке в чат
            if call:
                bot.send_message(call.message.chat.id, "❌ Произошла ошибка при загрузке настроек входа.")

    # --- ОБРАБОТКА ВВОДА ДЛЯ ИЗМЕНЕНИЯ ПОЛЕЙ ---
    @bot.message_handler(func=lambda message: message.from_user.id in user_states and user_states[message.from_user.id]['state'].startswith('awaiting_input_'))
    def handle_awaiting_input(message):
        user_id = message.from_user.id

        if user_id not in user_states or 'state' not in user_states[user_id]:
            handler.log_error(f"[INPUT] Пользователь {user_id} не в состоянии ожидания ввода")
            return

        state_info = user_states[user_id]
        state = state_info['state']
        account_id = state_info.get('account_id')
        field = state_info.get('field')
        chat_id = state_info.get('chat_id')
        message_id = state_info.get('message_id')

        if not all([account_id, field, chat_id, message_id]):
            handler.log_error(f"[INPUT] Отсутствуют необходимые данные для пользователя {user_id}: account_id={account_id}, field={field}, chat_id={chat_id}, message_id={message_id}")
            bot.send_message(message.chat.id, "❌ Произошла ошибка. Попробуйте снова.")
            user_states.pop(user_id, None)
            return

        new_value = message.text.strip()

        try:
            # Удаляем состояние ожидания
            user_states.pop(user_id, None)

            # Обновляем значение в базе данных
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # Проверяем, существует ли столбец
            cursor.execute(f"PRAGMA table_info(accounts)")
            columns = [column[1] for column in cursor.fetchall()]

            if field not in columns:
                conn.close()
                bot.send_message(message.chat.id, f"❌ Поле '{field}' не существует в базе данных.")
                return

            # Обновляем значение
            cursor.execute(f"UPDATE accounts SET {field} = ? WHERE id = ?", (new_value, account_id))
            conn.commit()
            conn.close()

            handler.log_info(f"[INPUT] Обновлено поле {field} для аккаунта {account_id}")

            # Имитируем создание call для cb_login_settings
            class MockCall:
                def __init__(self, message_id, chat_id, user_id, data):
                    self.message = type('obj', (object,), {
                        'message_id': message_id,
                        'chat': type('obj', (object,), {'id': chat_id})()
                    })()
                    self.id = user_id
                    self.data = data

            success_msg = bot.send_message(message.chat.id, f"✅ Поле '{field}' успешно обновлено!")

            # Удаляем сообщение об успехе через 3 секунды
            def delete_success_message():
                time.sleep(3)
                try:
                    bot.delete_message(message.chat.id, success_msg.message_id)
                except Exception as e:
                    handler.log_error("Ошибка при удалении сообщения об успехе", e)

            threading.Thread(target=delete_success_message, daemon=True).start()

            # Возвращаемся к меню настроек входа
            cb_login_settings(chat_id=chat_id, message_id=message_id, account_id=account_id)

        except Exception as e:
            handler.log_error(f"[INPUT] Ошибка при обновлении поля {field} для аккаунта {account_id}", e)
            bot.send_message(message.chat.id, f"❌ Произошла ошибка при обновлении поля '{field}'.")
            user_states.pop(user_id, None)

    # --- ИЗМЕНЕНИЕ ПОЛЕЙ АККАУНТА ---
    @bot.callback_query_handler(func=lambda call: call.data.startswith('change_field:'))
    def cb_change_field(call):
        try:
            # Парсим данные из callback
            parts = call.data.split(':')
            if len(parts) != 3:
                handler.log_error(f"[CHANGE_FIELD] Неверный формат callback_data: {call.data}")
                handler.answer_callback_query(call, "❌ Ошибка в данных")
                return

            account_id = parts[1]
            field = parts[2]

            # Определяем русское название поля для пользователя
            field_names = {
                'login': 'логин Steam',
                'password': 'пароль Steam',
                'email_login': 'почта',
                'email_password': 'пароль от почты',
                'imap_host': 'IMAP сервер'
            }

            field_name = field_names.get(field, field)

            # Сохраняем состояние пользователя
            user_states[call.from_user.id] = {
                'state': f'awaiting_input_{field}',
                'account_id': account_id,
                'field': field,
                'chat_id': call.message.chat.id,
                'message_id': call.message.message_id
            }

            # Создаем клавиатуру с кнопкой отмены
            keyboard = types.InlineKeyboardMarkup()
            keyboard.add(types.InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_input:{account_id}"))

            text = f"✏️ Введите новое значение для поля '{field_name}':"

            # Редактируем сообщение
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=text,
                reply_markup=keyboard
            )

            handler.answer_callback_query(call, f"Введите новое значение для '{field_name}'")

        except Exception as e:
            handler.log_error(f"[CHANGE_FIELD] Ошибка в cb_change_field для {call.data}", e)
            handler.answer_callback_query(call, "❌ Произошла ошибка")

    # --- ОТМЕНА ВВОДА ---
    @bot.callback_query_handler(func=lambda call: call.data.startswith('cancel_input:'))
    def cb_cancel_input(call):
        try:
            account_id = call.data.split(':')[1]
            user_id = call.from_user.id

            # Удаляем состояние ожидания ввода
            if user_id in user_states:
                user_states.pop(user_id, None)

            handler.answer_callback_query(call, "Ввод отменен")

            # Возвращаемся к меню настроек входа
            cb_login_settings(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                account_id=account_id
            )

        except Exception as e:
            handler.log_error(f"[CANCEL_INPUT] Ошибка в cb_cancel_input для {call.data}", e)
            handler.answer_callback_query(call, "❌ Произошла ошибка")