@echo off
setlocal

python.exe -m pip install --upgrade pip

:: venv
set VENV_DIR=venv311

:: check venv
if not exist "%VENV_DIR%\Scripts\activate.bat" (
    echo Создание виртуального окружения Python 3.11...
    py -3.11 -m venv %VENV_DIR%
)

:: venv
call "%VENV_DIR%\Scripts\activate.bat"

echo Установка зависимостей...
python.exe -m pip install --upgrade pip
pip install --upgrade pip setuptools wheel
pip install python-dotenv
pip install pydantic_core
pip install wheel
playwright install
pip install playwright
pip install imap-tools
pip install beautifulsoup4 lxml
pip install telebot

if exist requirements.txt (
    pip install -r requirements.txt
)

:: Проверка наличия .env
if not exist ".env" (
    echo Создание файла .env...
    echo TG_TOKEN=8154448487:AAH7nxbEcIHYz_Aev2OSDKaCgq_y16ZHxN8 > .env
    echo GOLDEN_KEY=uuxf9g39zf4n3lvuj89t1xks1ayzyjvd >> .env
    echo ADMIN_IDS=812130129 >> .env
    echo AUTHORIZED_TELEGRAM_IDS=812130129 >> .env
)

:: s
echo Установка браузеров Playwright...
python -m playwright install

echo Запуск бота аренды Steam-аккаунтов...
python standalone_steam_rental_bot.py

pause