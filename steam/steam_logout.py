import asyncio
from playwright.async_api import async_playwright
import os
import logging

logger = logging.getLogger("steam_logout")

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sessions')
os.makedirs(SESSIONS_DIR, exist_ok=True)

# Импортируем универсальную функцию из playwright_context
from .playwright_context import get_playwright_context

async def run_logout_async(login: str, password: str) -> bool:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context, page = await get_playwright_context(p, browser, login, password)
            logger.info("[STEAM_LOGOUT] Вход на страницу управления устройствами...")
            await page.goto("https://store.steampowered.com/account/authorizeddevices")
            await page.screenshot(path="step1_login.png")

            # Кнопка "Выйти из аккаунта везде"
            await page.wait_for_selector('button.DialogButton._DialogLayout.Small', timeout=15000)
            await page.screenshot(path="step5_authorized_devices.png")
            logger.info("[STEAM_LOGOUT] Клик по 'Выйти из аккаунта везде'...")
            await page.click('button.DialogButton._DialogLayout.Small')
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1)
            try:
                buttons = await page.query_selector_all('button')
                found = False
                for b in buttons:
                    text = (await b.inner_text()).strip().lower()
                    logger.info(f"[STEAM_LOGOUT] Найдена кнопка с текстом: {text}")
                    if any(x in text for x in ['remove all credentials', 'выйти', 'logout', 'sign out', 'remove credentials']):
                        await page.screenshot(path="step6_logout_confirm_text.png")
                        logger.info(f"[STEAM_LOGOUT] Клик по кнопке по тексту: {text}")
                        try:
                            await b.click()
                        except Exception:
                            await page.evaluate('(el) => el.click()', b)
                        found = True
                        break
                if not found:
                    logger.error('[STEAM_LOGOUT] Кнопка выхода не найдена!')
                    await page.screenshot(path='logout_fail_no_button.png')
                    return False
            except Exception as e:
                logger.error(f'[STEAM_LOGOUT] Ошибка поиска/клика по кнопке выхода: {e}')
                await page.screenshot(path='logout_fail_exception.png')
                return False
            await page.screenshot(path="step7_confirm_dialog.png")
            logger.info("[STEAM_LOGOUT] Ожидание подтверждающего окна...")
            proceed_found = False
            for b in await page.query_selector_all('button'):
                text = (await b.inner_text()).strip().lower()
                logger.info(f"[STEAM_LOGOUT] Кнопка подтверждения: {text}")
                if any(x in text for x in ['proceed', 'продолжить', 'ok', 'yes']):
                    await page.screenshot(path="step8_proceed_click.png")
                    logger.info(f"[STEAM_LOGOUT] Клик по подтверждающей кнопке: {text}")
                    try:
                        await b.click()
                    except Exception:
                        await page.evaluate('(el) => el.click()', b)
                    proceed_found = True
                    break
            if not proceed_found:
                logger.error('[STEAM_LOGOUT] Кнопка подтверждения (Proceed) не найдена!')
                await page.screenshot(path='logout_fail_no_proceed.png')
                return False
            await page.screenshot(path="step9_logout_done.png")

            await browser.close()
            logger.info(f"[STEAM_LOGOUT] Успешно выполнен выход из всех устройств для {login}")
            try:
                os.remove(os.path.join(SESSIONS_DIR, f"steam_{login}.json"))
                logger.info(f"[STEAM_SESSION] storage_state для {login} удалён после логаута.")
            except Exception as ex:
                logger.warning(f"[STEAM_SESSION] Не удалось удалить storage_state: {ex}")
            return True
    except Exception as e:
        logger.error(f"[STEAM_LOGOUT] Ошибка Playwright (async): {e}")
        try:
            html = await page.content()
            with open("logout_fail.html", "w", encoding="utf-8") as f:
                f.write(html)
        except Exception:
            pass
        return False

def steam_logout_all_sessions(login: str, password: str) -> bool:
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(run_logout_async(login, password), loop)
            return future.result()
        else:
            return loop.run_until_complete(run_logout_async(login, password))
    except Exception as e:
        logger.error(f"[STEAM_LOGOUT] Ошибка Playwright (async universal): {e}")
        return False
