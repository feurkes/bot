# Steam Account Rental Bot - Repository Analysis

## Overview
This repository contains a sophisticated Steam account rental automation system that integrates with the FunPay marketplace to provide automated Steam account rentals via a Telegram bot interface.

## System Architecture

### Core Components

#### 1. Main Application (`standalone_steam_rental_bot.py`)
- **Purpose**: Entry point for the entire system
- **Responsibilities**:
  - Initialize Telegram bot
  - Start FunPay integration listener
  - Initialize database and restore rental timers
  - Handle bot lifecycle and error recovery
- **Key Features**:
  - Automatic restart on errors
  - Admin notification system
  - Authorization middleware

#### 2. Telegram Bot System (`tg_utils/`)
- **handlers.py**: Main bot command handlers and user interaction logic
- **keyboards.py**: Telegram inline keyboard definitions
- **db.py**: Database initialization and management utilities
- **config.py**: Configuration management
- **state.py**: User session state management
- **helpers.py**: Utility functions for bot operations

#### 3. FunPay Integration (`funpay_integration.py`)
- **Purpose**: Automated order processing from FunPay marketplace
- **Key Features**:
  - Real-time order monitoring
  - Automatic account allocation
  - Customer communication automation
  - Review message handling
  - Order completion notifications

#### 4. Steam Account Management (`steam/`)
- **steam_account_rental_utils.py**: Core rental business logic
- **steam_password_changer.py**: Automated password changing using Playwright
- **steam_logout.py**: Session management and cleanup
- **steam_playwright_login.py**: Automated Steam login
- **accounts_navigation.py**: Steam interface navigation
- **playwright_context.py**: Browser context management

#### 5. Database Layer (`db/`, `tg_utils/db.py`)
- **SQLite-based** persistent storage
- **Main tables**:
  - `accounts`: Steam account information and rental status
  - `authorized_users`: User authorization data
  - `friend_mode_settings`: Temporary access configurations

#### 6. Utility Scripts
- **reset_rents.py**: Maintenance script to reset hanging rentals
- **reset_bonusgiven.py**: Reset bonus flags
- **game_name_mapper.py**: Game name standardization

## Business Logic Flow

### 1. Account Rental Process
```
User Request → Account Selection → Rental Duration → Payment → Account Delivery → Monitoring → Auto-Release
```

### 2. FunPay Integration Flow
```
FunPay Order → Order Parsing → Account Allocation → Customer Notification → Timer Setup → Completion
```

### 3. Steam Account Management
```
Account Pool → Rental Assignment → Password Change → Session Management → Release → Reset
```

## Technical Stack

### Core Technologies
- **Python 3.12**: Main programming language
- **SQLite**: Database for persistence
- **Playwright**: Browser automation for Steam interactions
- **Telegram Bot API**: User interface
- **FunPay API**: Marketplace integration

### Key Dependencies
- `pyTelegramBotAPI`: Telegram bot framework
- `playwright`: Browser automation
- `imap-tools`: Email integration for Steam Guard
- `python-dotenv`: Environment configuration
- `pytz`: Timezone handling
- `aiohttp`: Async HTTP client
- `beautifulsoup4`: HTML parsing

## Data Flow Analysis

### Account States
1. **free**: Available for rental
2. **rented**: Currently rented to a user
3. **maintenance**: Temporarily unavailable

### Rental Lifecycle
1. **Discovery**: Account marked as available
2. **Allocation**: Account assigned to user with time limit
3. **Monitoring**: Automated warnings and status checks
4. **Release**: Account freed and reset for next use

### Security Considerations
- Password rotation for each rental
- Session cleanup after use
- Steam Guard code automation
- User authorization controls

## Configuration Management

### Environment Variables (.env)
- `TELEGRAM_BOT_TOKEN`: Bot authentication
- `ADMIN_IDS`: Administrator user IDs
- `GOLDEN_KEY`: FunPay API key
- Email settings for Steam Guard integration
- Database path configuration

### Key Configuration Files
- `config.py`: Main configuration constants
- `tg_utils/config.py`: Telegram-specific settings
- `.env.example`: Configuration template

## Error Handling & Monitoring

### Logging System
- Structured logging with timestamps
- Different log levels (INFO, WARNING, ERROR)
- Error tracking and notification to admins

### Recovery Mechanisms
- Automatic bot restart on errors
- Database transaction retry logic
- Session cleanup on failures
- Rental timer restoration on startup

## Performance Considerations

### Scalability Features
- Threaded operations for concurrent users
- Database connection pooling
- Async operations where applicable
- Efficient account allocation algorithms

### Optimization Areas
- Browser automation efficiency
- Database query optimization
- Memory management for long-running processes

## Security Architecture

### Access Control
- User authorization system
- Admin-only commands
- Authorized user database

### Data Protection
- Credential encryption considerations
- Secure session management
- API key protection via environment variables

## Maintenance & Operations

### Automated Maintenance
- Rental timer cleanup
- Expired session removal
- Account status verification

### Manual Operations
- Account pool management
- User authorization updates
- System monitoring and debugging

## Integration Points

### External Systems
1. **Telegram API**: User interface and notifications
2. **FunPay API**: Order processing and marketplace integration
3. **Steam Web**: Account management and authentication
4. **Email Systems**: Steam Guard code retrieval

### Internal Modules
- Database operations
- State management
- Error handling
- Logging and monitoring

## Code Quality Observations

### Strengths
- Comprehensive error handling
- Modular architecture
- Extensive logging
- Robust database design
- Automated recovery mechanisms

### Areas for Improvement
- Code documentation could be enhanced
- Some large functions could be refactored
- Test coverage could be expanded
- Configuration validation could be strengthened

## Development Environment

### Setup Requirements
1. Python 3.12+
2. Playwright browser installation
3. Environment configuration (.env)
4. Database initialization
5. Required Python packages

### Testing Approach
- Manual testing via Telegram bot
- Database integrity checks
- Browser automation validation
- API integration testing

This analysis provides a comprehensive overview of the Steam Account Rental Bot system, highlighting its sophisticated architecture and automation capabilities.