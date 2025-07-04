"""
Конфигурация браузера для Playwright с поддержкой системного Chromium
"""
import shutil
import os
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright

def get_browser_config():
    """
    Возвращает конфигурацию для запуска браузера
    """
    config = {
        "headless": True,
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--disable-web-security",
            "--disable-features=VizDisplayCompositor",
            "--no-first-run",
            "--no-default-browser-check"
        ]
    }
    
    # Проверяем наличие системного chromium
    chromium_path = shutil.which("chromium")
    if chromium_path:
        config["executable_path"] = chromium_path
        print(f"[BROWSER] Используем системный Chromium: {chromium_path}")
    else:
        print("[BROWSER] Используем встроенный Chromium Playwright")
    
    return config

async def launch_browser_async():
    """
    Асинхронный запуск браузера с оптимальной конфигурацией
    """
    config = get_browser_config()
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(**config)
            print("[BROWSER] Браузер успешно запущен (async)")
            return browser
        except Exception as e:
            print(f"[BROWSER] Ошибка запуска браузера: {e}")
            # Fallback без executable_path
            fallback_config = {k: v for k, v in config.items() if k != "executable_path"}
            browser = await p.chromium.launch(**fallback_config)
            print("[BROWSER] Браузер запущен с fallback конфигурацией")
            return browser

def launch_browser_sync():
    """
    Синхронный запуск браузера с оптимальной конфигурацией
    """
    config = get_browser_config()
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(**config)
            print("[BROWSER] Браузер успешно запущен (sync)")
            return browser
        except Exception as e:
            print(f"[BROWSER] Ошибка запуска браузера: {e}")
            # Fallback без executable_path
            fallback_config = {k: v for k, v in config.items() if k != "executable_path"}
            browser = p.chromium.launch(**fallback_config)
            print("[BROWSER] Браузер запущен с fallback конфигурацией")
            return browser