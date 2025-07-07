import logging
import sys
from colorama import Fore, Style, init

# Инициализация colorama
init(autoreset=True)

# Стандартизированная цветовая схема логирования
LOG_STYLES = {
    'info': Fore.CYAN + Style.BRIGHT,
    'success': Fore.GREEN + Style.BRIGHT,
    'warning': Fore.YELLOW + Style.BRIGHT,
    'error': Fore.RED + Style.BRIGHT,
    'event': Fore.MAGENTA + Style.BRIGHT,
    'input': Fore.BLUE + Style.BRIGHT,
    'reset': Style.RESET_ALL
}

class ColoredFormatter(logging.Formatter):
    """Кастомный форматтер с цветами для разных уровней логирования"""
    
    # Цвета для разных уровней - используем стандартизированную схему
    COLORS = {
        'DEBUG': LOG_STYLES['input'],
        'INFO': LOG_STYLES['info'],
        'WARNING': LOG_STYLES['warning'],
        'ERROR': LOG_STYLES['error'],
        'CRITICAL': LOG_STYLES['event']
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
        color = self.COLORS.get(record.levelname, LOG_STYLES['info'])
        emoji = self.EMOJIS.get(record.levelname, '•')
        
        # Форматируем время
        timestamp = self.formatTime(record, datefmt='%Y-%m-%d %H:%M:%S')
        
        # Создаем цветное сообщение
        colored_message = (
            f"{color}{emoji} "
            f"[{timestamp}] "
            f"[{record.levelname}] "
            f"{record.name}: "
            f"{record.getMessage()}"
            f"{LOG_STYLES['reset']}"
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
    print(f"{LOG_STYLES['success']}✅ {message}{LOG_STYLES['reset']}")

def log_error(message):
    """Логирование ошибок"""
    print(f"{LOG_STYLES['error']}❌ {message}{LOG_STYLES['reset']}")

def log_warning(message):
    """Логирование предупреждений"""
    print(f"{LOG_STYLES['warning']}⚠️  {message}{LOG_STYLES['reset']}")

def log_info(message):
    """Логирование информации"""
    print(f"{LOG_STYLES['info']}🔵 {message}{LOG_STYLES['reset']}")

def log_event(message):
    """Логирование событий"""
    print(f"{LOG_STYLES['event']}🎯 {message}{LOG_STYLES['reset']}")

def log_input(message):
    """Логирование пользовательского ввода"""
    print(f"{LOG_STYLES['input']}📝 {message}{LOG_STYLES['reset']}")

# Создаем глобальный логгер
logger = setup_logger()