import os
import asyncio
from playwright.async_api import async_playwright
import os
from typing import Optional, Tuple
from playwright.async_api import Browser, BrowserContext, Page
from utils.logger import logger

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sessions')
os.makedirs(SESSIONS_DIR, exist_ok=True)

async def get_playwright_context(
    p, browser: Browser, login: str, password: str,
    *,
    session_prefix: str = "steam_",
    user_agent: Optional[str] = None,
    locale: str = "ru-RU",
    storage_dir: Optional[str] = None,
    debug_screens: bool = False
) -> Tuple[BrowserContext, Page]:
    """
    Универсальная функция для playwright-контекста Steam с поддержкой storage_state, логирования и расширенной кастомизации.
    :param p: playwright instance
    :param browser: playwright browser instance
    :param login: steam login
    :param password: steam password
    :param session_prefix: префикс для session-файла
    :param user_agent: кастомный User-Agent
    :param locale: локаль браузера
    :param storage_dir: директория для хранения сессий (по умолчанию SESSIONS_DIR)
    :param debug_screens: делать ли скриншоты для отладки
    :return: (context, page)
    """
    storage_dir = storage_dir or SESSIONS_DIR
    session_file = os.path.join(storage_dir, f"{session_prefix}{login}.json")
    context: Optional[BrowserContext] = None
    page: Optional[Page] = None
    logged_in = False
    if os.path.exists(session_file):
        logger.debug(f"[STEAM_SESSION] Найден session_file: {session_file}. Пытаемся использовать сохранённую сессию.")
        context = await browser.new_context(storage_state=session_file)
        page = await context.new_page()
        try:
            await page.goto("https://store.steampowered.com/account/")
            await page.wait_for_selector("#account_pulldown", timeout=10000)
            logged_in = True
        except Exception as e:
            logged_in = False
            logger.debug(f"[STEAM_SESSION] Повторный вход по session_state не удался: {e}")
        if not logged_in:
            logger.debug("[STEAM_SESSION] Сессия недействительна. Закрываем контекст и используем новый вход.")
            await context.close()
            context = None
    if context is None:
        logger.debug(f"[STEAM_SESSION] Создаем новый context и логинимся заново: login={login}")
        context = await browser.new_context(
            user_agent=user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport=None,
            locale=locale,
            java_script_enabled=True,
            ignore_https_errors=True
        )
        page = await context.new_page()
        await page.goto("https://store.steampowered.com/login/", wait_until="networkidle")
        if debug_screens:
            await page.screenshot(path=f"{login}_login_step1.png")
        await page.wait_for_selector('#input_username', timeout=15000)
        await page.fill('#input_username', login)
        await page.wait_for_selector('#input_password', timeout=15000)
        await page.fill('#input_password', password)
        await page.wait_for_selector('button[type="submit"]', timeout=15000)
        await page.click('button[type="submit"]')
        await page.wait_for_selector('#account_pulldown', timeout=30000)
        await context.storage_state(path=session_file)
    return context, page
