#!/usr/bin/env python3
"""
Скрипт для автоматической установки всех зависимостей проекта
Устанавливает библиотеки из requirements.txt и дополнительные зависимости
"""

import subprocess
import sys
import os
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

def run_command(command, description):
    """Выполнение команды с обработкой ошибок"""
    print_info(f"Выполняется: {description}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print_success(f"Завершено: {description}")
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"Ошибка при выполнении: {description}")
        print_error(f"Команда: {command}")
        print_error(f"Код ошибки: {e.returncode}")
        if e.stdout:
            print(f"Вывод: {e.stdout}")
        if e.stderr:
            print(f"Ошибки: {e.stderr}")
        return False

def install_dependencies():
    """Основная функция установки зависимостей"""
    print_bright("🚀 Начинаем установку зависимостей для Steam Rental Bot", Fore.MAGENTA)
    print()
    
    # Обновление pip
    if not run_command(f"{sys.executable} -m pip install --upgrade pip", "Обновление pip"):
        return False
    
    # Установка colorama (нужна для логирования)
    if not run_command(f"{sys.executable} -m pip install colorama", "Установка colorama"):
        return False
    
    # Установка основных зависимостей
    requirements_files = ["requirements.txt", "requirements_bot.txt"]
    
    for req_file in requirements_files:
        if os.path.exists(req_file):
            if not run_command(f"{sys.executable} -m pip install -r {req_file}", f"Установка зависимостей из {req_file}"):
                print_error(f"Не удалось установить зависимости из {req_file}")
                return False
        else:
            print_info(f"Файл {req_file} не найден, пропускаем")
    
    # Установка Playwright браузеров
    print_info("Установка браузеров Playwright...")
    if not run_command(f"{sys.executable} -m playwright install", "Установка браузеров Playwright"):
        print_error("Не удалось установить браузеры Playwright")
        return False
    
    # Дополнительные важные библиотеки
    additional_packages = [
        "python-dotenv",
        "pytz", 
        "pillow",
        "lxml",
        "beautifulsoup4",
        "requests"
    ]
    
    for package in additional_packages:
        run_command(f"{sys.executable} -m pip install {package}", f"Установка {package}")
    
    print()
    print_success("🎉 Установка зависимостей завершена!")
    print_bright("Теперь можно запустить first_setup.py для первоначальной настройки", Fore.YELLOW)
    
    return True

if __name__ == "__main__":
    try:
        success = install_dependencies()
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print_error("\n⚠️  Установка прервана пользователем")
        sys.exit(1)
    except Exception as e:
        print_error(f"Неожиданная ошибка: {e}")
        sys.exit(1)