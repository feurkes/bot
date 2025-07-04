import random
import string
import secrets

def generate_password(length=16, special_chars=False):
    """
    Генерация случайного пароля.
    Гарантирует наличие букв в верхнем и нижнем регистре, а также цифр.
    
    Args:
        length (int): Длина пароля (рекомендуется 16 для Steam)
        special_chars (bool): Добавлять ли специальные символы (не рекомендуется для Steam)
    
    Returns:
        str: Сгенерированный пароль
    """
    lower = string.ascii_lowercase
    upper = string.ascii_uppercase
    digits = string.digits
    
    password_chars = [
        secrets.choice(lower),
        secrets.choice(upper),
        secrets.choice(digits)
    ]

    all_chars = lower + upper + digits
    if special_chars:
        all_chars += "!@#$%^&*()-_=+[]{}|;:,.<>?"

    for _ in range(length - len(password_chars)):
        password_chars.append(secrets.choice(all_chars))

    secrets.SystemRandom().shuffle(password_chars)
    return ''.join(password_chars)
