"""Base handler class for Telegram bot handlers"""

from typing import Optional, Callable
from telebot import TeleBot
from tg_utils.logger import logger

class BaseHandler:
    """Base class for all bot handlers"""
    
    def __init__(self, bot: TeleBot, is_user_authorized: Optional[Callable] = None, 
                 auth_required: Optional[Callable] = None, admin_ids: Optional[list] = None):
        self.bot = bot
        self.is_user_authorized = is_user_authorized
        self.auth_required = auth_required or (lambda func: func)  # Default no-op decorator
        self.admin_ids = admin_ids or []
        self.logger = logger
    
    def log_error(self, message: str, error: Exception = None):
        """Log error message"""
        if error:
            self.logger.error(f"{message}: {error}")
        else:
            self.logger.error(message)
    
    def log_info(self, message: str):
        """Log info message"""
        self.logger.info(message)
    
    def log_debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)
    
    def send_error_message(self, chat_id: int, message: str = "❌ Произошла ошибка. Попробуйте позже."):
        """Send error message to user"""
        try:
            self.bot.send_message(chat_id, message)
        except Exception as e:
            self.log_error("Failed to send error message", e)
    
    def answer_callback_query(self, call, text: str = None, show_alert: bool = False):
        """Answer callback query with error handling"""
        try:
            self.bot.answer_callback_query(call.id, text, show_alert)
        except Exception as e:
            self.log_error("Failed to answer callback query", e)