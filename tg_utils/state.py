# next update 1
from typing import Dict, Any

# Словарь состояний пользователей: {user_id: state}
user_states: Dict[int, str] = {}

# Словарь данных аккаунтов при добавлении: {user_id: {field: value}}
user_acc_data: Dict[int, Dict[str, Any]] = {}

# Общий словарь данных пользователей: {user_id: {key: value}}
user_data: Dict[int, Dict[str, Any]] = {} 