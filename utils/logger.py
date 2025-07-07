"""
Legacy logger module - now redirects to tg_utils.logger for consistency
This maintains backward compatibility while using the improved logging system.
"""

# Import everything from the improved logger
from tg_utils.logger import (
    logger, 
    LOG_STYLES, 
    log_bright, 
    log_success, 
    log_error, 
    log_warning, 
    log_info, 
    log_event, 
    log_input,
    ColoredFormatter,
    setup_logger
)

# For backward compatibility, maintain the old interface
__all__ = [
    'logger', 
    'LOG_STYLES', 
    'log_bright', 
    'log_success', 
    'log_error', 
    'log_warning', 
    'log_info', 
    'log_event', 
    'log_input'
]
