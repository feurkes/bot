#!/usr/bin/env python3
"""
Простой стартер для Steam Rental Bot
Этот скрипт автоматически проверяет зависимости и запускает бота
"""

import subprocess
import sys
import os
from pathlib import Path

def check_requirements():
    """Проверка установленных зависимостей"""
    try:
        import colorama
        from colorama import Fore, Style
        colorama.init(autoreset=True)
        
        def print_bright(message, color=Fore.CYAN):
            print(f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}")
        
        def print_error(message):
            print(f"{Style.BRIGHT}{Fore.RED}❌ {message}{Style.RESET_ALL}")
            
        def print_success(message):
            print(f"{Style.BRIGHT}{Fore.GREEN}✅ {message}{Style.RESET_ALL}")
            
        def print_info(message):
            print(f"{Style.BRIGHT}{Fore.CYAN}🔵 {message}{Style.RESET_ALL}")
            
    except ImportError:
        # Fallback без цветов
        def print_bright(message, color=None):
            print(f">>> {message}")
        def print_error(message):
            print(f"ERROR: {message}")
        def print_success(message):
            print(f"SUCCESS: {message}")
        def print_info(message):
            print(f"INFO: {message}")
    
    print_bright("🚀 Steam Rental Bot Starter", Fore.MAGENTA if 'colorama' in sys.modules else None)
    print()
    
    # Проверяем основные модули
    required_modules = [
        ('python-dotenv', 'dotenv'),
        ('colorama', 'colorama'),
        ('pytelegrambotapi', 'telebot')
    ]
    
    missing_modules = []
    
    for module_name, import_name in required_modules:
        try:
            __import__(import_name)
            print_success(f"{module_name} установлен")
        except ImportError:
            print_error(f"{module_name} НЕ установлен")
            missing_modules.append(module_name)
    
    if missing_modules:
        print()
        print_info("Некоторые зависимости не установлены")
        print_info("Рекомендуется запустить install_requirements.py")
        print()
        response = input("Установить зависимости автоматически? (y/N): ").strip().lower()
        if response == 'y':
            return install_dependencies()
        else:
            print_error("Для работы бота необходимы все зависимости")
            return False
    
    return True

def install_dependencies():
    """Запуск установки зависимостей"""
    try:
        result = subprocess.run([sys.executable, "install_requirements.py"], check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        print("❌ Ошибка при установке зависимостей")
        return False
    except FileNotFoundError:
        print("❌ Файл install_requirements.py не найден")
        return False

def check_env_file():
    """Проверка файла .env"""
    if not os.path.exists(".env"):
        print("⚠️  Файл .env не найден")
        print("🔧 Запуск first_setup.py для первоначальной настройки...")
        try:
            subprocess.run([sys.executable, "first_setup.py"], check=True)
            return True
        except subprocess.CalledProcessError:
            print("❌ Ошибка при настройке")
            return False
        except FileNotFoundError:
            print("❌ Файл first_setup.py не найден")
            return False
    return True

def start_bot():
    """Запуск основного бота"""
    print()
    print("🤖 Запуск Steam Rental Bot...")
    try:
        subprocess.run([sys.executable, "main.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при запуске бота: {e}")
        return False
    except KeyboardInterrupt:
        print("\n⚠️  Бот остановлен пользователем")
        return True
    except FileNotFoundError:
        print("❌ Файл main.py не найден")
        return False
    
    return True

def main():
    """Главная функция стартера"""
    try:
        # Проверяем зависимости
        if not check_requirements():
            return False
        
        # Проверяем конфигурацию
        if not check_env_file():
            return False
        
        # Запускаем бота
        return start_bot()
        
    except KeyboardInterrupt:
        print("\n⚠️  Запуск прерван пользователем")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    success = main()
    input("\nНажмите Enter для выхода...")
    sys.exit(0 if success else 1)