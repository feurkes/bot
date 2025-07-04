import logging

def setup_logger():
    """Настраивает глобальный логгер для всего приложения"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger("steam_rental")

# Создаем глобальный логгер
logger = setup_logger()