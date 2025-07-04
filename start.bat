@echo off
echo ========================================
echo     Steam Rental Bot - Windows Setup
echo ========================================
echo.

:: Check for Python 3.11.9
python3.11 --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python 3.11 not found! Please install Python 3.11.9 from https://python.org
    pause
    exit /b 1
)

for /f "tokens=2 delims= " %%v in ('python3.11 --version') do set PY_VER=%%v
if not "%PY_VER%"=="3.11.9" (
    echo [ERROR] Python 3.11.9 is required! Found version %PY_VER%
    pause
    exit /b 1
)

echo [INFO] Python 3.11.9 found
python3.11 --version

:: Create virtual environment
echo.
echo [INFO] Creating virtual environment...
if not exist "venv" (
    python3.11 -m venv venv
    echo [OK] Virtual environment created
) else (
    echo [OK] Virtual environment already exists
)

:: Activate virtual environment
echo.
echo [INFO] Activating virtual environment...
call venv\Scripts\activate.bat

:: Upgrade pip
echo.
echo [INFO] Upgrading pip...
python -m pip install --upgrade pip

:: Install dependencies
echo.
echo [INFO] Installing dependencies...
pip install -r requirements_bot.txt

:: Check installation of critical dependencies
echo.
echo [INFO] Checking dependencies...
python -c "import telebot, playwright, cryptography, bcrypt; print('[OK] All main dependencies are installed')" 2>nul
if errorlevel 1 (
    echo [WARNING] Some dependencies are not installed. Trying to install without compilation...
    pip install --only-binary=cryptography,bcrypt,lxml cryptography bcrypt lxml
)

:: Install Playwright browsers
echo.
echo [INFO] Installing Playwright browsers...
playwright install chromium
if errorlevel 1 (
    echo [WARNING] Failed to install browsers. Try manually: playwright install chromium
)

:: Check .env file
echo.
if not exist ".env" (
    if exist ".env.example" (
        echo [INFO] Copying .env.example to .env...
        copy .env.example .env
        echo [WARNING] Edit the .env file with your own data!
    ) else (
        echo [WARNING] .env file not found. Please create it with the required variables.
    )
) else (
    echo [OK] .env file found
)

:: Start the bot
echo.
echo ========================================
echo        Starting Steam Rental Bot
echo ========================================
echo.
echo [INFO] Changing directory to bot14june...
cd bot14june

echo [INFO] Running the bot...
python standalone_steam_rental_bot.py

:: Handle exit status
echo.
echo ========================================
if errorlevel 1 (
    echo [ERROR] The bot exited with an error
) else (
    echo [INFO] The bot finished its work
)
echo ========================================
echo.
pause
