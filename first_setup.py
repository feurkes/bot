#!/usr/bin/env python3
"""
Скрипт для первоначальной настройки Steam Rental Bot
Создает .env файл, инициализирует базу данных и выполняет другие настройки
"""

import os
import sys
from colorama import Fore, Style, init

# Инициализация colorama
init(autoreset=True)

def print_bright(message, color=Fore.CYAN):
    """Печать яркого сообщения"""
    print(f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}")

def print_error(message):
    """Печать сообщения об ошибке"""
    print(f"{Style.BRIGHT}{Fore.RED}❌ {message}{Style.RESET_ALL}")

def print_success(message):
    """Печать сообщения об успехе"""
    print(f"{Style.BRIGHT}{Fore.GREEN}✅ {message}{Style.RESET_ALL}")

def print_info(message):
    """Печать информационного сообщения"""
    print(f"{Style.BRIGHT}{Fore.CYAN}🔵 {message}{Style.RESET_ALL}")

def print_warning(message):
    """Печать предупреждающего сообщения"""
    print(f"{Style.BRIGHT}{Fore.YELLOW}⚠️  {message}{Style.RESET_ALL}")

def create_env_file():
    """Создание .env файла с настройками"""
    print_bright("🔧 Настройка файла окружения (.env)", Fore.MAGENTA)
    
    if os.path.exists(".env"):
        print_warning("Файл .env уже существует")
        response = input(f"{Style.BRIGHT}{Fore.YELLOW}Перезаписать? (y/N): {Style.RESET_ALL}").strip().lower()
        if response != 'y':
            print_info("Пропускаем создание .env файла")
            return True
    
    # Сбор данных от пользователя
    print_info("Введите необходимые данные для настройки бота:")
    print()
    
    # Токен бота
    while True:
        bot_token = input(f"{Style.BRIGHT}{Fore.CYAN}Введите токен Telegram бота: {Style.RESET_ALL}").strip()
        if bot_token:
            break
        print_error("Токен не может быть пустым!")
    
    # ID администратора
    while True:
        try:
            admin_id = input(f"{Style.BRIGHT}{Fore.CYAN}Введите ваш Telegram ID (администратор): {Style.RESET_ALL}").strip()
            if admin_id:
                int(admin_id)  # Проверяем, что это число
                break
        except ValueError:
            pass
        print_error("Введите корректный числовой ID!")
    
    # Дополнительные настройки
    golden_key = input(f"{Style.BRIGHT}{Fore.CYAN}Golden Key (необязательно): {Style.RESET_ALL}").strip()
    
    # Создание .env файла
    env_content = f"""# Конфигурация Steam Rental Bot
TG_TOKEN={bot_token}
TELEGRAM_BOT_TOKEN={bot_token}

# Администраторы
ADMIN_IDS={admin_id}
AUTHORIZED_TELEGRAM_IDS={admin_id}

# Дополнительные настройки
GOLDEN_KEY={golden_key}

# Пути к базе данных
DB_PATH=./steam_rental.db
"""
    
    try:
        with open(".env", "w", encoding="utf-8") as f:
            f.write(env_content)
        print_success("Файл .env успешно создан!")
        return True
    except Exception as e:
        print_error(f"Ошибка при создании .env файла: {e}")
        return False

def create_directories():
    """Создание необходимых директорий"""
    print_bright("📁 Создание необходимых директорий", Fore.MAGENTA)
    
    directories = [
        "screenshots",
        "sessions", 
        "storage",
        "db"
    ]
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            print_success(f"Директория {directory} готова")
        except Exception as e:
            print_error(f"Не удалось создать директорию {directory}: {e}")
            return False
    
    return True

def initialize_database():
    """Инициализация базы данных"""
    print_bright("🗄️  Инициализация базы данных", Fore.MAGENTA)
    
    try:
        # Импортируем функции инициализации БД
        from tg_utils.db import init_db, ensure_accounts_columns
        
        print_info("Создание структуры базы данных...")
        init_db()
        ensure_accounts_columns()
        print_success("База данных успешно инициализирована!")
        return True
        
    except ImportError as e:
        print_error(f"Не удалось импортировать модули БД: {e}")
        print_warning("Возможно, нужно сначала запустить install_requirements.py")
        return False
    except Exception as e:
        print_error(f"Ошибка при инициализации БД: {e}")
        return False

def show_next_steps():
    """Показать следующие шаги"""
    print()
    print_bright("🎉 Первоначальная настройка завершена!", Fore.GREEN)
    print()
    print_bright("Следующие шаги:", Fore.YELLOW)
    print(f"{Style.BRIGHT}{Fore.CYAN}1. Запустите main.py для запуска бота{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}2. При необходимости настройте дополнительные параметры в .env{Style.RESET_ALL}")
    print(f"{Style.BRIGHT}{Fore.CYAN}3. Добавьте Steam аккаунты через бота{Style.RESET_ALL}")
    print()
    print_bright("Команда для запуска:", Fore.MAGENTA)
    print(f"{Style.BRIGHT}{Fore.WHITE}python main.py{Style.RESET_ALL}")

def main():
    """Основная функция настройки"""
    print_bright("🚀 Добро пожаловать в настройку Steam Rental Bot!", Fore.MAGENTA)
    print_bright("Этот скрипт поможет настроить бота для первого запуска", Fore.CYAN)
    print()
    
    steps = [
        ("Создание .env файла", create_env_file),
        ("Создание директорий", create_directories),
        ("Инициализация базы данных", initialize_database)
    ]
    
    for step_name, step_func in steps:
        print_bright(f"Шаг: {step_name}", Fore.YELLOW)
        if not step_func():
            print_error(f"Ошибка при выполнении шага: {step_name}")
            return False
        print()
    
    show_next_steps()
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print_error("\n⚠️  Настройка прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print_error(f"Неожиданная ошибка: {e}")
        sys.exit(1)