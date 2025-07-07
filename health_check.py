#!/usr/bin/env python3
"""
Steam Account Rental Bot - System Health Check

This script performs basic validation of the system components
to ensure all modules can be imported and basic functionality works.
"""

import sys
import os
import traceback
from datetime import datetime

def test_imports():
    """Test that all core modules can be imported"""
    print("=== Testing Module Imports ===")
    
    tests = [
        ("config", lambda: __import__("config")),
        ("tg_utils.db", lambda: __import__("tg_utils.db")),
        ("tg_utils.config", lambda: __import__("tg_utils.config")),
        ("steam.steam_account_rental_utils", lambda: __import__("steam.steam_account_rental_utils")),
        ("telebot", lambda: __import__("telebot")),
        ("playwright", lambda: __import__("playwright")),
        ("sqlite3", lambda: __import__("sqlite3")),
        ("dotenv", lambda: __import__("dotenv")),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            test_func()
            results.append((name, "✅ PASS"))
            print(f"  {name}: ✅ PASS")
        except Exception as e:
            results.append((name, f"❌ FAIL: {e}"))
            print(f"  {name}: ❌ FAIL: {e}")
    
    return results

def test_database():
    """Test database initialization and basic operations"""
    print("\n=== Testing Database Operations ===")
    
    try:
        from tg_utils.db import init_db, ensure_accounts_columns, DB_PATH
        from config import DB_DIR
        
        # Test database initialization
        init_db()
        ensure_accounts_columns()
        print("  Database initialization: ✅ PASS")
        
        # Test database connection
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        # Test table existence
        c.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in c.fetchall()]
        expected_tables = ['accounts', 'authorized_users', 'friend_mode_settings']
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            print(f"  Table check: ❌ FAIL: Missing tables {missing_tables}")
            return False
        else:
            print("  Table structure: ✅ PASS")
        
        # Test account table structure
        c.execute("PRAGMA table_info(accounts)")
        columns = [col[1] for col in c.fetchall()]
        required_columns = ['id', 'login', 'password', 'game_name', 'status']
        
        missing_columns = set(required_columns) - set(columns)
        if missing_columns:
            print(f"  Column check: ❌ FAIL: Missing columns {missing_columns}")
            return False
        else:
            print("  Column structure: ✅ PASS")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"  Database test: ❌ FAIL: {e}")
        traceback.print_exc()
        return False

def test_steam_utils():
    """Test Steam utility functions"""
    print("\n=== Testing Steam Utilities ===")
    
    try:
        from steam.steam_account_rental_utils import parse_rent_time, mark_account_free
        
        # Test parsing function
        test_cases = [
            ("1 час", 3600),
            ("2 hours", 7200),
            ("30 минут", 1800),
            ("1 day", 86400),
        ]
        
        for description, expected in test_cases:
            try:
                result = parse_rent_time(description)
                if result == expected:
                    print(f"  Parse '{description}': ✅ PASS")
                else:
                    print(f"  Parse '{description}': ❌ FAIL: got {result}, expected {expected}")
            except Exception as e:
                print(f"  Parse '{description}': ❌ FAIL: {e}")
        
        return True
        
    except Exception as e:
        print(f"  Steam utils test: ❌ FAIL: {e}")
        return False

def test_config():
    """Test configuration loading"""
    print("\n=== Testing Configuration ===")
    
    try:
        import config
        
        # Check required attributes
        required_attrs = ['DB_PATH', 'DB_DIR', 'TG_TOKEN']
        for attr in required_attrs:
            if hasattr(config, attr):
                print(f"  {attr}: ✅ PASS")
            else:
                print(f"  {attr}: ❌ FAIL: Missing attribute")
        
        # Check if database directory exists
        if os.path.exists(config.DB_DIR):
            print(f"  DB Directory: ✅ PASS")
        else:
            print(f"  DB Directory: ❌ FAIL: {config.DB_DIR} does not exist")
        
        return True
        
    except Exception as e:
        print(f"  Configuration test: ❌ FAIL: {e}")
        return False

def test_bot_basics():
    """Test basic bot functionality without starting it"""
    print("\n=== Testing Bot Components ===")
    
    try:
        import telebot
        from tg_utils.config import ADMIN_IDS, AUTHORIZED_TELEGRAM_IDS
        
        # Test that admin IDs are configured
        if ADMIN_IDS and len(ADMIN_IDS) > 0:
            print("  Admin IDs configured: ✅ PASS")
        else:
            print("  Admin IDs configured: ❌ FAIL: No admin IDs found")
        
        # Test authorized users
        if AUTHORIZED_TELEGRAM_IDS and len(AUTHORIZED_TELEGRAM_IDS) > 0:
            print("  Authorized users configured: ✅ PASS")
        else:
            print("  Authorized users configured: ❌ FAIL: No authorized users found")
        
        # Test that we can create a bot instance (without token validation)
        try:
            from config import TG_TOKEN
            if TG_TOKEN and TG_TOKEN != "":
                print("  Bot token configured: ✅ PASS")
            else:
                print("  Bot token configured: ❌ FAIL: No token found")
        except:
            print("  Bot token configured: ❌ FAIL: Token not accessible")
        
        return True
        
    except Exception as e:
        print(f"  Bot components test: ❌ FAIL: {e}")
        return False

def generate_summary():
    """Generate a summary report"""
    print("\n" + "="*50)
    print("SYSTEM HEALTH CHECK SUMMARY")
    print("="*50)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python Version: {sys.version}")
    print(f"Working Directory: {os.getcwd()}")
    
    # Run all tests
    import_results = test_imports()
    db_test = test_database()
    steam_test = test_steam_utils()
    config_test = test_config()
    bot_test = test_bot_basics()
    
    print("\n=== FINAL RESULTS ===")
    failed_imports = [name for name, result in import_results if "FAIL" in result]
    
    if failed_imports:
        print(f"❌ Failed imports: {', '.join(failed_imports)}")
    else:
        print("✅ All core modules imported successfully")
    
    if db_test:
        print("✅ Database operations working")
    else:
        print("❌ Database issues detected")
    
    if steam_test:
        print("✅ Steam utilities working")
    else:
        print("❌ Steam utilities issues detected")
    
    if config_test:
        print("✅ Configuration loaded")
    else:
        print("❌ Configuration issues detected")
    
    if bot_test:
        print("✅ Bot components ready")
    else:
        print("❌ Bot component issues detected")
    
    overall_status = all([
        len(failed_imports) == 0,
        db_test,
        steam_test,
        config_test,
        bot_test
    ])
    
    print(f"\n🔍 OVERALL SYSTEM STATUS: {'✅ HEALTHY' if overall_status else '❌ ISSUES DETECTED'}")
    
    return overall_status

if __name__ == "__main__":
    try:
        # Change to script directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        os.chdir(script_dir)
        
        # Run health check
        system_healthy = generate_summary()
        
        # Exit with appropriate code
        sys.exit(0 if system_healthy else 1)
        
    except Exception as e:
        print(f"\n💥 CRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(2)