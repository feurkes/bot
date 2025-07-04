from telebot import TeleBot, types
import logging
from typing import Any, Optional

logger = logging.getLogger("steam_rental") # next update

def safe_edit_message_text(bot: TeleBot, chat_id: int, message_id: int, text: str, reply_markup=None, parse_mode=None) -> None:
    """
    Безопасно изменяет текст сообщения, игнорируя ошибку "message is not modified".
    
    Args:
        bot (TeleBot): Экземпляр бота.
        text (str): Новый текст сообщения
        chat_id (int): ID чата
        message_id (int): ID сообщения
        reply_markup: Reply markup for the message
        parse_mode: Parse mode for the message
    """
    try:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка при обновлении сообщения: {e}")
            try:
                bot.edit_message_reply_markup(chat_id, message_id, reply_markup=reply_markup)
            except Exception as e2:
                logger.error(f"Не удалось обновить клавиатуру: {e2}")
                try:
                    bot.send_message(chat_id, text, reply_markup=reply_markup, parse_mode=parse_mode)
                except Exception as e3:
                    logger.error(f"Не удалось отправить новое сообщение: {e3}")

def safe_edit_message_media(bot, chat_id, message_id, media_url, caption=None, reply_markup=None, parse_mode=None): # next update
    try:
        bot.edit_message_media(
            chat_id=chat_id,
            message_id=message_id,
            media=types.InputMediaAnimation(
                media=media_url, # next update 2
                caption=caption,
                parse_mode=parse_mode
            ),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка при обновлении медиа: {e}")
        try:
            bot.edit_message_reply_markup(chat_id, message_id, reply_markup=reply_markup)
        except Exception as e2:
            logger.error(f"Не удалось обновить клавиатуру: {e2}")
            try:
                bot.send_animation(
                    chat_id,
                    media_url,
                    caption=caption, # next update 3
                    parse_mode=parse_mode,
                    reply_markup=reply_markup
                )
            except Exception as e3:
                logger.error(f"Не удалось отправить новое сообщение: {e3}")  