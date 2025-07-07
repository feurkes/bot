import logging
import sys
from colorama import Fore, Style, init

# Инициализация colorama
init(autoreset=True)

class ColoredFormatter(logging.Formatter):
    """Кастомный форматтер с цветами для разных уровней логирования"""
    
    # Цвета для разных уровней
    COLORS = {
        'DEBUG': Fore.BLUE,
        'INFO': Fore.CYAN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.MAGENTA
    }
    
    # Эмодзи для разных уровней
    EMOJIS = {
        'DEBUG': '🔍',
        'INFO': '🔵',
        'WARNING': '⚠️ ',
        'ERROR': '❌',
        'CRITICAL': '🚨'
    }
    
    def format(self, record):
        # Получаем цвет и эмодзи для уровня
        color = self.COLORS.get(record.levelname, Fore.WHITE)
        emoji = self.EMOJIS.get(record.levelname, '•')
        
        # Форматируем время
        timestamp = self.formatTime(record, datefmt='%Y-%m-%d %H:%M:%S')
        
        # Создаем цветное сообщение
        colored_message = (
            f"{Style.BRIGHT}{color}{emoji} "
            f"[{timestamp}] "
            f"[{record.levelname}] "
            f"{record.name}: "
            f"{record.getMessage()}"
            f"{Style.RESET_ALL}"
        )
        
        return colored_message

def setup_logger():
    """Настраивает глобальный логгер с цветным выводом"""
    
    # Создаем логгер
    logger = logging.getLogger("steam_rental")
    logger.setLevel(logging.INFO)
    
    # Удаляем существующие handlers, если есть
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Создаем консольный handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Устанавливаем цветной форматтер
    formatter = ColoredFormatter()
    console_handler.setFormatter(formatter)
    
    # Добавляем handler к логгеру
    logger.addHandler(console_handler)
    
    # Предотвращаем дублирование сообщений
    logger.propagate = False
    
    return logger

def log_bright(message, level='INFO', color=Fore.CYAN):
    """Функция для яркого логирования с кастомным цветом"""
    print(f"{Style.BRIGHT}{color}{message}{Style.RESET_ALL}")

def log_success(message):
    """Логирование успешных операций"""
    log_bright(f"✅ {message}", color=Fore.GREEN)

def log_error(message):
    """Логирование ошибок"""
    log_bright(f"❌ {message}", color=Fore.RED)

def log_warning(message):
    """Логирование предупреждений"""
    log_bright(f"⚠️  {message}", color=Fore.YELLOW)

def log_info(message):
    """Логирование информации"""
    log_bright(f"🔵 {message}", color=Fore.CYAN)

# Создаем глобальный логгер
logger = setup_logger()