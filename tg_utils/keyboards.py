# Улучшенные клавиатуры с детальными описаниями
from telebot import types

def main_menu():
    """Главное меню с подробными описаниями"""
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("📋 Управление аккаунтами", callback_data="list_accs"))
    kb.add(types.InlineKeyboardButton("➕ Добавить новый аккаунт", callback_data="add_acc"))
    kb.add(types.InlineKeyboardButton("📊 Статистика и аналитика", callback_data="stats"))
    kb.add(types.InlineKeyboardButton("⚙️ Настройки системы", callback_data="settings"))
    kb.add(types.InlineKeyboardButton("💬 Техподдержка", url="https://t.me/AnastasiaPisun"))
    return kb

def account_kb(acc_id, status, game, index):
    """Клавиатура для управления конкретным аккаунтом"""
    kb = types.InlineKeyboardMarkup()
    
    # Основные действия с аккаунтом
    kb.add(types.InlineKeyboardButton("🧪 Проверить работоспособность", callback_data=f"test:{acc_id}"))
    kb.add(types.InlineKeyboardButton("📝 Изменить логин/пароль", callback_data=f"chgdata:{acc_id}"))
    kb.add(types.InlineKeyboardButton("📮 Получить код", callback_data=f"get_code:{acc_id}"))
    
    # Переключение Steam Guard
    guard_button_text_placeholder = "🔐 Переключить Steam Guard"
    kb.add(types.InlineKeyboardButton(guard_button_text_placeholder, callback_data=f"toggle_guard:{acc_id}:{game}:{index}"))

    # Действия в зависимости от статуса
    if status == "free":
        kb.add(types.InlineKeyboardButton("🟢 Сдать в аренду", callback_data=f"rent:{acc_id}"))
        kb.add(types.InlineKeyboardButton("🗑 Удалить аккаунт", callback_data=f"del:{acc_id}"))
    else:
        kb.add(types.InlineKeyboardButton("⏹ Завершить аренду досрочно", callback_data=f"return:{acc_id}"))
        
    return kb 

def game_selection_kb(games_list):
    """Клавиатура для выбора игры с улучшенным дизайном"""
    kb = types.InlineKeyboardMarkup()
    
    # Группируем игры по 2 в ряд для компактности
    for i in range(0, len(games_list), 2):
        row = []
        for j in range(i, min(i + 2, len(games_list))):
            game = games_list[j]
            # Добавляем эмодзи для популярных игр
            emoji = get_game_emoji(game)
            row.append(types.InlineKeyboardButton(f"{emoji} {game}", callback_data=f"select_game:{game}"))
        kb.row(*row)
    
    kb.add(types.InlineKeyboardButton("🔙 Назад в главное меню", callback_data="main_menu"))
    return kb

def rental_time_kb():
    """Клавиатура для выбора времени аренды с детальными опциями"""
    kb = types.InlineKeyboardMarkup()
    
    # Популярные варианты времени
    kb.row(
        types.InlineKeyboardButton("⚡️ 1 час (быстро)", callback_data="rent_time:1"),
        types.InlineKeyboardButton("🎯 3 часа (оптимально)", callback_data="rent_time:3")
    )
    kb.row(
        types.InlineKeyboardButton("🎮 6 часов (игровая сессия)", callback_data="rent_time:6"),
        types.InlineKeyboardButton("🌙 12 часов (полдня)", callback_data="rent_time:12")
    )
    kb.add(types.InlineKeyboardButton("🌅 24 часа (сутки)", callback_data="rent_time:24"))
    kb.add(types.InlineKeyboardButton("⏰ Свое время (от 6 мин до 30 дней)", callback_data="rent_time:custom"))
    kb.add(types.InlineKeyboardButton("🔙 Отмена", callback_data="cancel_rent"))
    
    return kb

def confirmation_kb(action, acc_id):
    """Клавиатура подтверждения действия"""
    kb = types.InlineKeyboardMarkup()
    
    action_texts = {
        "delete": "🗑 Да, удалить аккаунт",
        "logout": "⛔️ Да, выйти из Steam", 
        "return": "⏹ Да, завершить аренду",
        "test": "🧪 Да, запустить тест"
    }
    
    kb.add(types.InlineKeyboardButton(action_texts.get(action, "✅ Подтвердить"), 
                                    callback_data=f"confirm_{action}:{acc_id}"))
    kb.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel"))
    
    return kb

def navigation_kb(current_page, total_pages, game_name, has_prev, has_next):
    """Навигационная клавиатура для списков аккаунтов"""
    kb = types.InlineKeyboardMarkup()
    
    # Навигация по страницам
    nav_row = []
    if has_prev:
        nav_row.append(types.InlineKeyboardButton("⬅️ Предыдущая", 
                                                callback_data=f"page_prev:{game_name}:{current_page-1}"))
    if has_next:
        nav_row.append(types.InlineKeyboardButton("➡️ Следующая", 
                                                callback_data=f"page_next:{game_name}:{current_page+1}"))
    
    if nav_row:
        kb.row(*nav_row)
    
    # Индикатор страницы
    if total_pages > 1:
        kb.add(types.InlineKeyboardButton(f"📄 Страница {current_page + 1} из {total_pages}", 
                                        callback_data="noop"))
    
    # Кнопка возврата
    kb.add(types.InlineKeyboardButton("🔙 К выбору игр", callback_data="list_accs"))
    kb.add(types.InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu"))
    
    return kb

def stats_kb():
    """Клавиатура для статистики"""
    kb = types.InlineKeyboardMarkup()
    
    kb.add(types.InlineKeyboardButton("📈 Статистика аренды", callback_data="rental_stats"))
    kb.add(types.InlineKeyboardButton("💰 Финансовая сводка", callback_data="financial_stats"))
    kb.add(types.InlineKeyboardButton("🎮 По играм", callback_data="game_stats"))
    kb.add(types.InlineKeyboardButton("📊 Популярные аккаунты", callback_data="popular_accounts"))
    kb.add(types.InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu"))
    
    return kb

def settings_kb():
    """Клавиатура настроек системы"""
    kb = types.InlineKeyboardMarkup()
    
    kb.add(types.InlineKeyboardButton("🔔 Настройки уведомлений", callback_data="notification_settings"))
    kb.add(types.InlineKeyboardButton("⏰ Автозавершение аренды", callback_data="auto_end_settings"))
    kb.add(types.InlineKeyboardButton("🔐 Безопасность", callback_data="security_settings"))
    kb.add(types.InlineKeyboardButton("💾 Резервное копирование", callback_data="backup_settings"))
    kb.add(types.InlineKeyboardButton("🔙 Главное меню", callback_data="main_menu"))
    
    return kb

def get_game_emoji(game_name):
    """Возвращает эмодзи для популярных игр"""
    game_emojis = {
        "CS2": "🔫",
        "DOTA 2": "⚔️", 
        "PUBG": "🎯",
        "Apex Legends": "🚁",
        "Valorant": "💥",
        "Fortnite": "🏗️",
        "Minecraft": "⛏️",
        "GTA": "🚗",
        "Call of Duty": "🎖️",
        "Rocket League": "🚀"
    }
    
    return game_emojis.get(game_name, "🎮")

def back_to_account_kb(acc_id, game, index):
    """Кнопка возврата к аккаунту"""
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔙 Назад к аккаунту", 
                                    callback_data=f"back_to_acc:{acc_id}:{game}:{index}"))
    return kb