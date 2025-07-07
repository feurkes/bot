# API Integrations and Workflow Analysis

## Telegram Bot API Integration

### Command Structure
The bot implements a comprehensive command and callback system:

#### Core Commands
- `/start`, `/menu` - Main menu and bot initialization
- Account management (add, test, modify accounts)
- Rental management (view active rentals, extend, end)
- Admin commands (user management, system stats)

#### Callback Query Handlers
- `test:` - Account testing functionality
- `refresh_menu` - Refresh main menu
- `back_to_menu` - Navigation back to main menu
- `add_acc` - Add new Steam account wizard
- `imap_preset:` - IMAP configuration presets
- Account selection and rental callbacks

#### State Management
The bot uses a sophisticated state management system:
- `user_states` - Tracks current user workflow state
- `user_acc_data` - Stores temporary account data during creation
- `user_data` - Persistent user session data

States include: `add_id`, `add_login`, `add_password`, `add_game`, `add_mail`, etc.

### User Authentication & Authorization
- Hard-coded admin IDs in configuration
- User authorization check middleware
- Access denial with appropriate messages
- Security through Telegram user ID verification

## FunPay API Integration

### Order Processing Workflow
```python
FunPay Order Event → Order Parsing → Account Allocation → Customer Notification
```

#### Key Features:
1. **Real-time Order Monitoring**: Listens for new orders from FunPay
2. **Automatic Order Processing**: Parses order details and allocates accounts
3. **Customer Communication**: Sends account details directly to buyers
4. **Review Handling**: Processes and responds to customer reviews
5. **Order Completion**: Automated completion notifications

#### Order Event Types:
- `NewOrderEvent` - Processes new incoming orders
- `NewMessageEvent` - Handles customer messages and communications

### Message Parsing Logic
The system includes sophisticated parsing for:
- Game names and rental durations
- Customer communications
- Review messages and feedback
- Account extension requests

## Steam Integration Workflows

### Account Management Pipeline
```
Account Pool → Selection → Login → Password Change → Session Management → Cleanup
```

### Browser Automation (Playwright)
#### Steam Login Process:
1. **Session Initialization**: Create clean browser context
2. **Navigation**: Access Steam login page
3. **Credential Input**: Automated form filling
4. **2FA Handling**: Steam Guard code retrieval from email
5. **Session Verification**: Confirm successful login
6. **Context Preservation**: Save session state

#### Password Change Automation:
1. **Authentication**: Login to Steam account
2. **Navigation**: Access account settings
3. **Current Password**: Input existing credentials
4. **New Password**: Generate and set new password
5. **Confirmation**: Verify password change success
6. **Database Update**: Store new credentials

### Email Integration for Steam Guard
#### IMAP Email Processing:
1. **Connection**: Connect to email server (Gmail, Outlook, etc.)
2. **Search**: Look for Steam Guard emails
3. **Parse**: Extract verification codes
4. **Cleanup**: Remove processed emails
5. **Code Return**: Provide code to automation system

#### Supported Email Providers:
- Gmail (imap.gmail.com)
- Outlook (outlook.office365.com)
- Yahoo (imap.mail.yahoo.com)
- Custom IMAP servers

## Database Operations & Data Flow

### Account Lifecycle Management
```sql
-- Account states: free → rented → maintenance → free
UPDATE accounts SET 
    status='rented', 
    tg_user_id=?, 
    rented_until=?, 
    order_id=?,
    warned_10min=0 
WHERE id=?
```

### Rental Timer System
- **Rental Duration**: Configurable rental periods
- **Warning System**: 10-minute warnings before expiration
- **Auto-release**: Automatic account freeing on timer expiry
- **Timer Persistence**: Restored on system restart

### Data Consistency
- **Transaction Management**: Ensures database consistency
- **Retry Logic**: Handles database locks and temporary failures
- **Backup/Recovery**: Account state restoration mechanisms

## Error Handling & Recovery

### Multi-level Error Handling
1. **Function Level**: Try-catch blocks with specific error handling
2. **System Level**: Global exception handlers and recovery
3. **User Level**: Friendly error messages and fallback options
4. **Admin Level**: Error notifications and system alerts

### Recovery Mechanisms
- **Automatic Restart**: Bot restarts on critical errors
- **State Recovery**: User session restoration
- **Timer Recovery**: Rental timer restoration on startup
- **Database Recovery**: Transaction rollback on failures

### Logging Strategy
```python
logger.info(f"[RENT] Account {account_id} rented until {timestamp}")
logger.warning(f"[ERROR] Database connection failed, retrying...")
logger.error(f"[CRITICAL] System component failure: {error}")
```

## Performance & Scalability

### Concurrent Operations
- **Threading**: Multiple user operations simultaneously
- **Async Operations**: Non-blocking I/O for browser automation
- **Connection Pooling**: Efficient database connection management
- **Queue Management**: Order processing queue system

### Resource Management
- **Browser Context**: Efficient Playwright browser management
- **Memory Management**: Proper cleanup of browser sessions
- **Database Connections**: Connection lifecycle management
- **File Handling**: Temporary file cleanup

## Security Considerations

### Data Protection
- **Credential Security**: Steam account passwords
- **Email Integration**: IMAP credentials protection
- **API Keys**: FunPay and other service keys
- **User Data**: Telegram user information

### Access Control
- **Admin Authorization**: Restricted admin commands
- **User Verification**: Telegram ID-based authentication
- **Session Security**: Browser session isolation
- **Database Security**: SQL injection prevention

### Privacy Measures
- **Data Minimization**: Only necessary data storage
- **Temporary Data**: Cleanup of sensitive temporary files
- **Logging**: Sensitive data exclusion from logs
- **Encryption**: Where applicable, credential protection

## Configuration Management

### Environment Variables
```env
TELEGRAM_BOT_TOKEN=     # Bot authentication
ADMIN_IDS=              # Authorized administrators
GOLDEN_KEY=             # FunPay API key
EMAIL_LOGIN=            # Steam Guard email
EMAIL_PASSWORD=         # Email password
IMAP_HOST=              # Email server
DATABASE_PATH=          # Database location
```

### Runtime Configuration
- **Database Paths**: Configurable storage location
- **Logging Levels**: Adjustable verbosity
- **Timeout Settings**: Browser and network timeouts
- **Retry Logic**: Configurable retry attempts

This analysis demonstrates the sophisticated integration patterns and robust workflow management implemented in the Steam Account Rental Bot system.