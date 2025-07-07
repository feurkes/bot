# Steam Account Rental Bot - Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           STEAM ACCOUNT RENTAL BOT                             │
│                                ARCHITECTURE                                     │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                               EXTERNAL SYSTEMS                                 │
├─────────────────┬─────────────────┬─────────────────┬─────────────────────────┤
│   Telegram API  │   FunPay API    │   Steam Web     │   Email Servers         │
│                 │                 │                 │   (IMAP)                │
│   - Bot API     │   - Order API   │   - Login       │   - Gmail               │
│   - Messages    │   - Chat API    │   - Settings    │   - Outlook             │
│   - Callbacks   │   - Reviews     │   - Guard       │   - Yahoo               │
└─────────────────┴─────────────────┴─────────────────┴─────────────────────────┘
         │                 │                 │                 │
         │                 │                 │                 │
         ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            APPLICATION LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────┐  │
│  │  Telegram Bot   │    │ FunPay Listener │    │     Browser Automation      │  │
│  │   Interface     │    │                 │    │                             │  │
│  │                 │    │ - Order Monitor │    │ - Playwright Integration    │  │
│  │ - Command Hdlrs │    │ - Auto Response │    │ - Steam Login/Logout        │  │
│  │ - Callbacks     │    │ - Chat Mgmt     │    │ - Password Changes          │  │
│  │ - State Mgmt    │    │ - Reviews       │    │ - Session Management        │  │
│  │ - User Auth     │    │                 │    │                             │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────────┘  │
│           │                       │                           │                  │
│           └───────────────────────┼───────────────────────────┘                  │
│                                   │                                              │
└───────────────────────────────────┼──────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                             BUSINESS LOGIC LAYER                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────┐  │
│  │  Rental Manager │    │ Account Manager │    │     Timer & Scheduler       │  │
│  │                 │    │                 │    │                             │  │
│  │ - Rent Logic    │    │ - Pool Mgmt     │    │ - Rental Timers             │  │
│  │ - Time Parsing  │    │ - State Tracking│    │ - Warning System            │  │
│  │ - Auto Release  │    │ - Allocation    │    │ - Auto-Release              │  │
│  │ - Extensions    │    │ - Availability  │    │ - Timer Recovery            │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────────┘  │
│           │                       │                           │                  │
│           └───────────────────────┼───────────────────────────┘                  │
│                                   │                                              │
└───────────────────────────────────┼──────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                               DATA LAYER                                       │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │                        SQLite Database                                     │  │
│  │                                                                             │  │
│  │  ┌─────────────────┐ ┌──────────────────┐ ┌──────────────────────────────┐ │  │
│  │  │    accounts     │ │ authorized_users │ │   friend_mode_settings       │ │  │
│  │  │                 │ │                  │ │                              │ │  │
│  │  │ - id (PK)       │ │ - user_id (PK)   │ │ - tg_user_id (PK)           │ │  │
│  │  │ - login         │ │ - is_authorized  │ │ - activated_at               │ │  │
│  │  │ - password      │ │ - access_attempts│ │ - is_active                  │ │  │
│  │  │ - game_name     │ │ - last_attempt   │ │                              │ │  │
│  │  │ - status        │ │                  │ │                              │ │  │
│  │  │ - rented_until  │ │                  │ │                              │ │  │
│  │  │ - tg_user_id    │ │                  │ │                              │ │  │
│  │  │ - email_*       │ │                  │ │                              │ │  │
│  │  │ - order_id      │ │                  │ │                              │ │  │
│  │  │ - steam_guard   │ │                  │ │                              │ │  │
│  │  │ - warned_10min  │ │                  │ │                              │ │  │
│  │  │ - bonus_given   │ │                  │ │                              │ │  │
│  │  │ - friend_mode   │ │                  │ │                              │ │  │
│  │  │ - rented_by     │ │                  │ │                              │ │  │
│  │  └─────────────────┘ └──────────────────┘ └──────────────────────────────┘ │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                            UTILITY & SUPPORT LAYER                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────────┐  │
│  │   Logging &     │    │   Config Mgmt   │    │       Utilities             │  │
│  │   Monitoring    │    │                 │    │                             │  │
│  │                 │    │ - Environment   │    │ - Email Utils               │  │
│  │ - Error Tracking│    │ - DB Paths      │    │ - Password Generation       │  │
│  │ - Admin Alerts  │    │ - API Keys      │    │ - Game Name Mapping         │  │
│  │ - Debug Logs    │    │ - User Auth     │    │ - Browser Config            │  │
│  │ - Performance   │    │                 │    │ - Reset Scripts             │  │
│  └─────────────────┘    └─────────────────┘    └─────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                               DATA FLOW                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  User Order (FunPay) ──► Order Processing ──► Account Allocation ──►           │
│                                                                                 │
│  Account Delivery ──► Timer Setup ──► Monitoring ──► Warnings ──► Auto-Release │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────────┐  │
│  │                          SECURITY MEASURES                                  │  │
│  │                                                                             │  │
│  │  • User Authentication via Telegram ID                                     │  │
│  │  • Admin Authorization Controls                                            │  │
│  │  • Password Rotation per Rental                                            │  │
│  │  • Session Isolation & Cleanup                                             │  │
│  │  • Secure Email Integration                                                │  │
│  │  • API Key Protection                                                      │  │
│  │  • Database Transaction Safety                                             │  │
│  └─────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Architecture Patterns

### 1. **Layered Architecture**
   - External Systems Layer
   - Application Layer
   - Business Logic Layer
   - Data Layer
   - Utility & Support Layer

### 2. **Event-Driven Design**
   - FunPay order events trigger automated workflows
   - Timer-based events for rental management
   - User interaction events via Telegram

### 3. **Modular Design**
   - Separate modules for each major functionality
   - Clear separation of concerns
   - Pluggable components (FunPay, Steam, Email)

### 4. **State Management**
   - Database-backed persistent state
   - In-memory session state for user interactions
   - Timer state for rental management

### 5. **Error Handling & Recovery**
   - Multi-level error handling
   - Automatic recovery mechanisms
   - Admin alerting system

### 6. **Security-First Approach**
   - Authentication and authorization layers
   - Secure credential management
   - Session isolation and cleanup