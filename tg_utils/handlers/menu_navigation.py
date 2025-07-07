"""Menu navigation handlers for Telegram bot"""

import sqlite3
from .base import BaseHandler
from tg_utils.db import DB_PATH
from tg_utils.keyboards import main_menu, stats_kb, settings_kb, get_game_emoji
from tg_utils.helpers import safe_edit_message_text


def init_menu_navigation_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """Initialize menu navigation handlers"""
    
    # Create handler instance
    handler = BaseHandler(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    bot = bot_instance
    is_user_authorized = is_user_authorized_func
    auth_required = auth_required_decorator or (lambda func: func)

    @bot.message_handler(commands=['start', 'menu'])
    def cmd_start(message):
        if is_user_authorized and is_user_authorized(message.chat.id):
            welcome_text = (
                "🚀 <b>Steam Rental Bot v2.0</b>\n\n"
                "💡 <b>Добро пожаловать в систему управления арендой Steam аккаунтов!</b>\n\n"
                "📋 <b>Управление аккаунтами</b> - просмотр, добавление и настройка аккаунтов\n"
                "➕ <b>Добавить новый аккаунт</b> - регистрация нового Steam аккаунта\n"
                "📊 <b>Статистика и аналитика</b> - отчеты по аренде и доходам\n"
                "⚙️ <b>Настройки системы</b> - конфигурация уведомлений и безопасности\n"
                "💬 <b>Техподдержка</b> - связь с администратором\n\n"
                "🔥 Выберите действие для начала работы:"
            )
            bot.send_message(message.chat.id, welcome_text, reply_markup=main_menu(), parse_mode="HTML")
        else:
            unauthorized_text = (
                "⛔ <b>Доступ ограничен</b>\n\n"
                "🔐 Этот бот предназначен только для авторизованных администраторов системы аренды Steam аккаунтов.\n\n"
                "📞 Для получения доступа обратитесь к администратору."
            )
            bot.send_message(message.chat.id, unauthorized_text, parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "refresh_menu")
    @auth_required
    def cb_refresh(call):
        handler.answer_callback_query(call)
        bot.edit_message_text("👾 <b>Steam Rental 1.0.3</b>\nУправляй аккаунтами и арендой!", 
                            call.message.chat.id, call.message.message_id, 
                            reply_markup=main_menu(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "back_to_menu")
    @auth_required
    def cb_back(call):
        handler.answer_callback_query(call)
        bot.edit_message_text("👾 <b>Steam Rental 1.0.3</b>\nУправляй аккаунтами и арендой!", 
                            call.message.chat.id, call.message.message_id, 
                            reply_markup=main_menu(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "main_menu")
    @auth_required
    def cb_main_menu(call):
        """Возврат в главное меню"""
        handler.answer_callback_query(call)
        welcome_text = (
            "🚀 <b>Steam Rental Bot v2.0</b>\n\n"
            "💡 <b>Добро пожаловать в систему управления арендой Steam аккаунтов!</b>\n\n"
            "📋 <b>Управление аккаунтами</b> - просмотр, добавление и настройка аккаунтов\n"
            "➕ <b>Добавить новый аккаунт</b> - регистрация нового Steam аккаунта\n"
            "📊 <b>Статистика и аналитика</b> - отчеты по аренде и доходам\n"
            "⚙️ <b>Настройки системы</b> - конфигурация уведомлений и безопасности\n"
            "💬 <b>Техподдержка</b> - связь с администратором\n\n"
            "🔥 Выберите действие для начала работы:"
        )
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               welcome_text, main_menu(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "stats")
    @auth_required
    def cb_stats_menu(call):
        """Показать меню статистики"""
        handler.answer_callback_query(call)
        
        # Получаем основную статистику из базы данных
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            # Общее количество аккаунтов
            c.execute("SELECT COUNT(*) FROM accounts")
            total_accounts = c.fetchone()[0]
            
            # Количество свободных аккаунтов
            c.execute("SELECT COUNT(*) FROM accounts WHERE status = 'free'")
            free_accounts = c.fetchone()[0]
            
            # Количество арендованных аккаунтов
            rented_accounts = total_accounts - free_accounts
            
            conn.close()
            
            stats_text = (
                f"📊 <b>Статистика системы</b>\n\n"
                f"📈 <b>Общий обзор:</b>\n"
                f"• Всего аккаунтов: <b>{total_accounts}</b>\n"
                f"• Свободных: <b>{free_accounts}</b>\n"
                f"• В аренде: <b>{rented_accounts}</b>\n\n"
                f"📋 Выберите тип отчета для подробной информации:"
            )
            
        except Exception as e:
            handler.log_error("Error fetching stats", e)
            stats_text = (
                "📊 <b>Статистика системы</b>\n\n"
                "❌ Ошибка при загрузке данных\n\n"
                "📈 Выберите тип отчета:"
            )
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               stats_text, stats_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "settings")
    @auth_required  
    def cb_settings_menu(call):
        """Показать меню настроек"""
        handler.answer_callback_query(call)
        
        settings_text = (
            "⚙️ <b>Настройки системы</b>\n\n"
            "🔧 <b>Конфигурация Steam Rental Bot</b>\n\n"
            "🔔 <b>Настройки уведомлений</b> - управление оповещениями\n"
            "⏰ <b>Автозавершение аренды</b> - настройка таймеров\n"
            "🔐 <b>Безопасность</b> - управление доступом и защитой\n"
            "💾 <b>Резервное копирование</b> - настройка бэкапов\n\n"
            "⚡ Выберите категорию для настройки:"
        )
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               settings_text, settings_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data in ["rental_stats", "financial_stats", "game_stats", "popular_accounts"])
    @auth_required
    def cb_detailed_stats(call):
        """Обработчик детальной статистики"""
        handler.answer_callback_query(call)
        
        stat_type = call.data
        
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            
            if stat_type == "rental_stats":
                # Статистика аренды - считаем активные аренды
                c.execute("SELECT COUNT(*) FROM accounts WHERE status != 'free'")
                active_rentals = c.fetchone()[0]
                
                stats_text = (
                    f"📈 <b>Статистика аренды</b>\n\n"
                    f"🔥 <b>Активных аренд:</b> {active_rentals}\n"
                    f"⏰ <b>Система автозавершения:</b> Активна\n"
                    f"🔄 <b>Автосмена паролей:</b> Включена\n\n"
                    f"📋 Подробная статистика в разработке..."
                )
                
            elif stat_type == "game_stats":
                # Статистика по играм
                c.execute("SELECT game_name, COUNT(*) FROM accounts GROUP BY game_name ORDER BY COUNT(*) DESC")
                games_data = c.fetchall()
                
                stats_text = "🎮 <b>Статистика по играм</b>\n\n"
                for game, count in games_data:
                    emoji = get_game_emoji(game)
                    stats_text += f"{emoji} <b>{game}:</b> {count} акк.\n"
                
                if not games_data:
                    stats_text += "📋 Пока нет добавленных аккаунтов"
                    
            elif stat_type == "financial_stats":
                stats_text = (
                    "💰 <b>Финансовая статистика</b>\n\n"
                    "📊 Данная функция находится в разработке\n\n"
                    "🔜 Скоро будет доступно:\n"
                    "• Доходы за период\n"
                    "• Популярные игры\n"
                    "• Средняя длительность аренды"
                )
                
            elif stat_type == "popular_accounts":
                # Популярные аккаунты (по количеству аренд)
                stats_text = (
                    "⭐ <b>Популярные аккаунты</b>\n\n"
                    "📊 Данная функция находится в разработке\n\n"
                    "🔜 Скоро будет показывать:\n"
                    "• Самые арендуемые аккаунты\n"
                    "• Рейтинг по играм\n"
                    "• Доходность аккаунтов"
                )
            
            conn.close()
            
        except Exception as e:
            handler.log_error("Error fetching detailed stats", e)
            stats_text = f"❌ Ошибка при загрузке статистики: {str(e)}"
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               stats_text, stats_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data in ["notification_settings", "auto_end_settings", "security_settings", "backup_settings"])
    @auth_required
    def cb_settings_category(call):
        """Обработчик категорий настроек"""
        handler.answer_callback_query(call)
        
        category = call.data
        
        if category == "notification_settings":
            settings_text = (
                "🔔 <b>Настройки уведомлений</b>\n\n"
                "📨 <b>Конфигурация оповещений</b>\n\n"
                "✅ <b>Уведомления о новых арендах:</b> Включены\n"
                "✅ <b>Уведомления об окончании аренды:</b> Включены\n"
                "✅ <b>Уведомления об ошибках:</b> Включены\n\n"
                "⚙️ Настройка уведомлений в разработке..."
            )
        elif category == "auto_end_settings":
            settings_text = (
                "⏰ <b>Автозавершение аренды</b>\n\n"
                "🔄 <b>Система автосмены паролей</b>\n\n"
                "✅ <b>Автозавершение:</b> Активно\n"
                "✅ <b>Смена паролей:</b> Включена\n"
                "⏱️ <b>Проверка каждые:</b> 5 минут\n\n"
                "⚙️ Настройка таймеров в разработке..."
            )
        elif category == "security_settings":
            settings_text = (
                "🔐 <b>Настройки безопасности</b>\n\n"
                "🛡️ <b>Защита и доступ</b>\n\n"
                f"👥 <b>Авторизованных админов:</b> {len(admin_ids or [])}\n"
                "🔒 <b>Двухфакторная аутентификация:</b> Steam Guard\n"
                "🔄 <b>Автосмена паролей:</b> Активна\n\n"
                "⚙️ Настройка безопасности в разработке..."
            )
        elif category == "backup_settings":
            settings_text = (
                "💾 <b>Резервное копирование</b>\n\n"
                "🗂️ <b>Управление бэкапами</b>\n\n"
                "📅 <b>Автобэкап:</b> Ежедневно\n"
                "📁 <b>Путь сохранения:</b> ./backup/\n"
                "🔢 <b>Хранить копий:</b> 7 дней\n\n"
                "⚙️ Настройка бэкапов в разработке..."
            )
        
        safe_edit_message_text(bot, call.message.chat.id, call.message.message_id, 
                               settings_text, settings_kb(), parse_mode="HTML")

    @bot.callback_query_handler(func=lambda c: c.data == "noop")
    def cb_noop(call):
        """No-operation callback handler"""
        handler.answer_callback_query(call, "⚙️ Функция в разработке")