# Telegram Bot Handlers Package
from .account_management import *
from .account_settings import *
from .steam_operations import *
from .menu_navigation import *
from .utils import *

# Main initialization function
def init_handlers(bot_instance, is_user_authorized_func=None, auth_required_decorator=None, admin_ids=None):
    """Initialize all bot handlers"""
    from .account_management import init_account_management_handlers
    from .account_settings import init_account_settings_handlers
    from .steam_operations import init_steam_operations_handlers
    from .menu_navigation import init_menu_navigation_handlers
    
    # Initialize all handler modules
    init_account_management_handlers(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    init_account_settings_handlers(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    init_steam_operations_handlers(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)
    init_menu_navigation_handlers(bot_instance, is_user_authorized_func, auth_required_decorator, admin_ids)