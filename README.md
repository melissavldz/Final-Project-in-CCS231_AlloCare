# Allocare - Hospital Resource Allocation System

Allocare is a healthcare resource management platform designed for real-time tracking of patient admissions, bed occupancy, and staff availability. The system uses an asynchronous architecture with RabbitMQ to ensure reliable message delivery and high availability.

## System Overview

The platform consists of three main components:
- **Publisher Subsystem**: A Flask web application where administrators manage hospital resources (patients, beds, doctors). Every action publishes an event to RabbitMQ.
- **Message Broker**: RabbitMQ serves as the central hub, using durable queues to ensure events are never lost.
- **Subscriber Subsystem**: A separate consumer service that processes events from RabbitMQ, stores them in a persistent database, and provides a real-time monitoring dashboard for subscribers.

## Setup and Run Instructions

Follow these steps to get the system running on your local machine.

### Prerequisites
- **Docker & Docker Compose**: Installed and running.
- **Python 3.11+**: Optional, for local script execution outside of Docker.

### 1. Project Setup
Clone the repository and navigate to the project root:
```powershell
cd "c:\Users\Documents\Allocare"
```

### 2. Launch the System
The easiest way to run the entire stack (Flask apps, Consumer, and RabbitMQ) is using Docker Compose:
```powershell
docker-compose up --build
```

### 3. Accessing the System
Once the containers are running, you can access the different components:

- **Admin/Publisher UI**: [http://localhost:5000](http://localhost:5000)
  - Tasks: Admit patients, update bed status, register doctors.
- **Subscriber UI**: [http://localhost:5000/subscriber-login](http://localhost:5000/subscriber-login)
  - Tasks: Monitor real-time events, view critical alerts, and browse event history.
- **RabbitMQ Management**: [http://localhost:15672](http://localhost:15672) (Credentials: `guest` / `guest`)
  - Tasks: Monitor queue levels and message throughput.

### 4. Stopping the System
To stop all services and remove the containers:
```powershell
docker-compose down
```


### Database Schema

#### Publisher Database
- `users`: Publisher authentication
- `patient_admissions`: Admitted patient records
- `bed_updates`: Bed status changes
- `facility_capacities`: Max capacity per unit
- `doctors`: Doctor registrations

#### Subscriber Database
- `subscribers`: Subscriber authentication
- `consumed_events`: All consumed RabbitMQ events (persistent log)
- `alerts`: Critical events requiring attention
- `queue_subscriptions`: Topic filtering subscriptions

### RabbitMQ Message Broker Queues

All queues are configured with durability and persistence flags.

| Queue Name | Purpose | Event Types |
|-----------|---------|------------|
| allocare.patient.admission | Patient admission tracking | admission, discharge, transfer, emergency |
| allocare.bed.updates | Bed status management | available, occupied, maintenance |
| allocare.bed.capacity | Facility capacity configuration | capacity_update, capacity_warning |
| allocare.doctor.registration | Doctor system management | registered, deregistered, availability_changed |
| allocare.auth.events | Authentication logging | login, logout, signup, session_created |
| allocare.filesystem.events | Configuration and data file monitoring | created, modified, deleted |

### Consumer Service (consumer.py)

**Continuous Background Service** that:
1. Connects to all 5 RabbitMQ queues
2. Processes messages with manual acknowledgment
3. Stores events in `consumed_events` table
4. Generates alerts for critical events
5. Ensures at-least-once delivery guarantee

**Key Settings**:
- Prefetch count: 1 (process one at a time)
- Queue durability: True (survive broker restarts)
- Acknowledgment: Manual (ack only after DB storage)
- Reconnect backoff: 5 seconds

## Key Features Explained

### 1. At-Least-Once Delivery

```python
# Consumer processes and stores FIRST
store_consumed_event(queue_name, message)

# THEN acknowledges (only if successful)
ch.basic_ack(delivery_tag=method.delivery_tag)

# If consumer crashes between steps: message requeued
# Result: Event processed at least once, never lost
```

### 2. Topic Filtering

Subscribers can filter by:
- **Queue Name** (allocare.patient.admission, bed.updates, etc.)
- **Event Type** (patient.admission, bed.update, etc.)
- **Date Range** (when did event occur)
- **Severity** (critical/high/medium/low for admissions)
- **Facility Unit** (ICU, general ward, etc.)

```sql
SELECT * FROM consumed_events
WHERE queue_name = 'allocare.patient.admission'
  AND event_type = 'patient.admission'
  AND consumed_at > DATE('now', '-7 days');
```

### 3. Multicast Distribution

All subscribers connected to same queue receive same message:

```
Publisher sends: "Patient P123 admitted (critical)"
                 ↓
           allocare.patient.admission queue
                 ↓
    ┌────────────┼────────────┐
    ↓            ↓            ↓
Subscriber1  Subscriber2  Subscriber3
(all get same event)
```

### 4. Persistent Message Storage

Events remain in database permanently:
- Historical audit trail
- Replay capability
- Compliance/reporting
- Trend analysis

## Installation & Setup Guide

### System Requirements

Ensure your system meets the following minimum requirements:

| Component | Requirement |
|-----------|-------------|
| Operating System | Windows 10/11, Linux, or macOS with WSL2 |
| Docker Engine | Version 20.10+ |
| Docker Compose | Version 1.29+ |
| Python | Version 3.11+ (for local development) |
| Disk Space | Minimum 2 GB free |
| Memory | Minimum 4 GB RAM |

**Verification:**
```powershell
docker --version
python --version
docker compose --version
```

### Step 1: Clone or Download Project

```powershell
# Option A: If using Git
git clone <repository-url>
cd Allocare

# Option B: Or extract from zip file
cd c:\Users\Documents\Allocare
```

### Step 2: Set Up Python Virtual Environment (Optional but Recommended)

For local development without Docker:

```powershell
# Navigate to project directory
cd c:\Users\Documents\Allocare

# Create virtual environment
python -m venv .venv

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# If execution policy error, run:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

# Install dependencies
pip install -r requirements.txt
```

**Note:** If you only want to use Docker, skip Steps 2-4 and go to Step 5.

### Step 3: Configure Environment Variables (Optional)

Create a `.env` file in the project root (optional - defaults are provided):

```powershell
# Create .env file
notepad .env
```

Add these lines:
```
SECRET_KEY=your-secret-key-for-production
DATABASE_PATH=data/allocare.db
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
DEBUG=True
```

Save and close.

### Step 4: Initialize Database (Local Development Only)

If running locally without Docker, initialize the database:

```powershell
# Activate virtual environment first
.\.venv\Scripts\Activate.ps1

# Run Python to initialize database
python -c "from app import init_db; init_db()"
```

### Step 5: Run with Docker Compose (Recommended - All in One)

This is the easiest way to run the entire system:

```powershell
# Navigate to project directory
cd c:\Users\Documents\Allocare

# Build and start all services
docker compose up --build
```

**What this does:**
- Builds Docker images for Flask app and consumer service
- Starts RabbitMQ broker (accessible at http://localhost:15672)
- Starts Flask web application (http://localhost:5000)
- Starts consumer service (background process)
- Creates and initializes SQLite database

**Expected Output:**
```
[+] Running 3/3
  Container allocare-rabbitmq    Running
  Container allocare-web         Running
  Container allocare-consumer    Running
```

**Keep terminal open** - this keeps all services running. To stop:
```powershell
# Press Ctrl+C or in another terminal:
docker compose down
```

### Step 6: Access the System

Once all services are running, open your browser and visit:

#### Publisher System
**URL:** http://localhost:5000

**Step 1: Initial Configuration**

1. Navigate to the sign-up page
2. Create publisher account using credentials of choice
3. Return to login page and authenticate
4. Access the Publisher Dashboard

**Publisher Features:**
- **Dashboard**: View real-time analytics of published events
- **Patient Admission**: Publish patient admission events
- **Bed Management**: Configure hospital capacity and update bed status
- **Doctor Registration**: Register doctors with specialties

#### Subscriber System
**URL:** http://localhost:5000/subscriber-login

**Step 1: Initial Configuration**

1. Navigate to the subscriber sign-up page
2. Create subscriber account using credentials of choice
3. Return to login page and authenticate
4. Access the Subscriber Dashboard

**Subscriber Features:**
- **Dashboard**: View consumed events and analytics
- **Alerts**: See critical alerts (critical patients, capacity issues, staff changes)
- **Event History**: Browse, search, and filter all consumed events

#### RabbitMQ Management Console
**URL:** http://localhost:15672

**Login Credentials:**
- Username: `guest`
- Password: `guest`

**View:**
- Queue status and message counts
- Message publish/consumption rates
- Active consumers and connections
- Event history in queues

### Step 7: Verify Everything is Working

**Test Publisher → RabbitMQ → Subscriber Flow:**

1. **Go to Publisher Dashboard** (http://localhost:5000)
   - Navigate to "Patient Admission"
   - Fill form:
     - Patient ID: `P001`
     - Severity: `critical` (important for alert test)
     - Facility Unit: `ICU`
     - Patient Name: `John Doe`
   - Click "Submit Admission"
   - Should redirect to dashboard

2. **Check RabbitMQ Console** (http://localhost:15672)
   - Look at "Queues" tab
   - Find `allocare.patient.admission` queue
   - Should show 1 message (or 0 if consumer already processed)

3. **Check Consumer Logs**
   ```powershell
   docker compose logs allocare-consumer --tail 10
   ```
   - Should show: `[*] Received from allocare.patient.admission: patient.admission`
   - Should show: `[SUCCESS] Acknowledged delivery_tag: 1`

4. **Go to Subscriber Dashboard** (http://localhost:5000/subscriber-dashboard)
   - Should show "Patient Admissions" count increased by 1
   - Recent events feed should show the admission
   - "Total Events Consumed" should be > 0

5. **Check Alerts Page** (http://localhost:5000/subscriber-alerts)
   - Should show at least one alert with type "Critical Patient"
   - Severity should show "CRITICAL" in red

6. **Check History Page** (http://localhost:5000/subscriber-history)
   - Should list the consumed event
   - Event type should be "patient.admission"
   - Status should be "acknowledged"

If all above steps work, your system is fully operational and ready for production use.

## Development Workflow

### Local Development (Without Docker)

```bash
# Terminal 1: Start RabbitMQ (via Docker)
docker run -d -p 5672:5672 -p 15672:15672 \
  -e RABBITMQ_DEFAULT_USER=guest \
  -e RABBITMQ_DEFAULT_PASS=guest \
  rabbitmq:3-management

# Terminal 2: Start Flask app
python app.py

# Terminal 3: Start consumer service
python consumer.py

# Access at http://localhost:5000
```

### Environment Variables

Create `.env` file in project root:
```
SECRET_KEY=your-secret-key-change-for-production
DATABASE_PATH=data/allocare.db
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASS=guest
```

Then in PowerShell:
```powershell
# Terminal 1: Start RabbitMQ container
docker run -d -p 5672:5672 -p 15672:15672 `
  -e RABBITMQ_DEFAULT_USER=guest `
  -e RABBITMQ_DEFAULT_PASS=guest `
  rabbitmq:3-management

# Terminal 2: Activate venv and start Flask app
.\.venv\Scripts\Activate.ps1
python app.py

# Terminal 3: Activate venv and start consumer
.\.venv\Scripts\Activate.ps1
python consumer.py
```

### Database Initialization

Database automatically initializes on first run. To reset:

```powershell
# Remove database
Remove-Item data/allocare.db -Force

# Restart app
python app.py
# or
docker compose restart allocare-web
```

## API Routes

### Publisher Routes

| Route | Method | Description |
|-------|--------|-------------|
| `/` | GET | Redirect to dashboard if logged in |
| `/login` | GET/POST | Publisher login |
| `/signup` | GET/POST | Publisher registration |
| `/logout` | POST | Clear session |
| `/publisher-dashboard` | GET | Analytics dashboard |
| `/patient-admission` | GET/POST | Patient admission form |
| `/bed-management` | GET/POST | Bed/capacity management |
| `/doctor-registration` | GET/POST | Doctor registration |
| `/health` | GET | Service health check |

### Subscriber Routes

| Route | Method | Description |
|-------|--------|-------------|
| `/subscriber-login` | GET/POST | Subscriber login |
| `/subscriber-signup` | GET/POST | Subscriber registration |
| `/subscriber-dashboard` | GET | Event analytics |
| `/subscriber-alerts` | GET | Critical alerts |
| `/subscriber-history` | GET | Event history with filtering |
| `/subscriber-logout` | POST | Clear subscriber session |

## Common Issues & Solutions

### Error: Docker daemon not running

**Solution:**
1. Open Docker Desktop application
2. Wait for it to fully load (check system tray for Docker icon)
3. Try again: `docker compose up --build`

### Error: Port 5000 already in use

**Solution:**
```powershell
# Find process using port 5000
netstat -ano | findstr :5000

# Kill process (replace PID with actual number)
taskkill /PID <PID> /F

# Or use Docker to kill existing container
docker compose down
docker compose up --build
```

### Error: Port 5672 already in use

**Solution:**
```powershell
# Stop all Docker containers
docker compose down

# Or stop specific RabbitMQ container
docker stop allocare-rabbitmq
docker rm allocare-rabbitmq

# Restart
docker compose up --build
```

### Error: Connection refused to localhost

**Solution:**
1. Verify Docker containers are running:
   ```powershell
   docker compose ps
   ```
   Should show 3 containers with STATUS "Up"

2. Check if Flask app is initialized:
   ```powershell
   docker compose logs allocare-web --tail 20
   ```
   Should show: `Running on http://0.0.0.0:5000`

3. Restart services:
   ```powershell
   docker compose restart allocare-web
   ```

### Error: Consumer not receiving messages

**Solution:**
1. Check if consumer is running:
   ```powershell
   docker compose ps
   # allocare-consumer should show "Up" status
   ```

2. Check consumer logs:
   ```powershell
   docker compose logs allocare-consumer --tail 20
   # Should show "Consumer started. Waiting for messages..."
   ```

3. Restart consumer:
   ```powershell
   docker compose restart allocare-consumer
   ```

4. Verify RabbitMQ connectivity:
   - Open http://localhost:15672
   - Login with guest/guest
   - Check if queues exist and have messages

### Error: Database locked

**Solution:**
```powershell
# Restart the web service
docker compose restart allocare-web

# Or completely restart everything
docker compose down
docker compose up --build -d
```

### Error: Flask not showing template changes

**Solution:**
1. Clear Python cache:
   ```powershell
   Get-ChildItem -Path __pycache__ -Recurse -Force | Remove-Item -Recurse -Force
   ```

2. Clear browser cache:
   - Open browser DevTools: F12
   - Press Ctrl+Shift+Delete
   - Clear "All time" data
   - Close and reopen browser

3. Restart Flask:
   ```powershell
   docker compose restart allocare-web
   ```

### Error: Module not found or Import error

**Solution:**
```powershell
# Reinstall dependencies
pip install --upgrade -r requirements.txt

# Or rebuild Docker images (most reliable)
docker compose down
docker compose up --build
```

### Error: Subscriber not seeing published events

**Checklist:**
1. Did you publish an event from Publisher? Check dashboard.
2. Did you create a Subscriber account? Check /subscriber-login.
3. Is consumer running? Check `docker compose logs allocare-consumer`.
4. Wait 1-2 seconds for consumer to process.
5. Refresh Subscriber Dashboard page: F5

**Debug steps:**
```powershell
# 1. Check database has consumed_events
sqlite3 data/allocare.db "SELECT COUNT(*) FROM consumed_events;"

# 2. Check RabbitMQ has messages in queues
# Visit http://localhost:15672 -> Queues tab

# 3. Check consumer is connected
docker compose logs allocare-consumer --tail 5
```

### Error: Alerts not appearing on Alerts page

**Checklist:**
1. Did you admit a patient with severity="critical"?
2. Check database has alerts:
   ```powershell
   sqlite3 data/allocare.db "SELECT * FROM alerts;"
   ```
3. Alerts only show for critical events - try:
   - Patient admission with severity="critical"
   - Doctor registration with availability="off-duty"

## Performance & Scaling

### Message Processing Rate
- Consumer handles ~100-1000 msg/sec depending on DB
- RabbitMQ can handle ~50K msg/sec
- Bottleneck: Database writes

### Scaling Strategies

1. **Multiple Consumers**
   - Add more consumer instances in docker-compose
   - All share same database
   - RabbitMQ distributes load

2. **Database Optimization**
   - Add indexes on frequently filtered columns
   - Archive old events to separate storage
   - Use connection pooling

3. **Queue Optimization**
   - Separate high-volume queues
   - Implement message TTL for auto-cleanup
   - Use dead-letter exchanges for failed messages

## File Structure

```
allocare/
├── app.py                      # Flask app (publisher + subscriber routes)
├── consumer.py                 # Consumer service
├── docker-compose.yml          # Multi-service orchestration
├── Dockerfile                  # Container build
├── requirements.txt            # Python dependencies
├── SUBSCRIBER_SYSTEM.md        # Detailed subscriber docs
├── README.md                   # This file
│
├── templates/                  # HTML templates
│   ├── login.html             # Publisher login
│   ├── signup.html            # Publisher signup
│   ├── dashboard.html         # Publisher dashboard
│   ├── patient-admission.html # Patient form
│   ├── bed-management.html    # Bed management
│   ├── doctor-registration.html # Doctor form
│   ├── subscriber-login.html      # Subscriber login
│   ├── subscriber-signup.html     # Subscriber signup
│   ├── subscriber-dashboard.html  # Subscriber dashboard
│   ├── subscriber-alerts.html     # Alerts page
│   └── subscriber-history.html    # Event history
│
├── static/css/                # Stylesheets
│   ├── styles.css             # Auth pages
│   ├── base-dashboard.css     # Layout & sidebar
│   ├── dashboard.css          # Publisher dashboard
│   ├── patient-admission.css  # Patient form
│   ├── bed-management.css     # Bed form
│   ├── doctor-registration.css # Doctor form
│   ├── subscriber-dashboard.css # Subscriber dashboard
│   ├── subscriber-alerts.css   # Alerts styling
│   └── subscriber-history.css  # History styling
│
└── data/
    └── allocare.db            # SQLite database (auto-created)
```

## Security Considerations

### Production Deployment

1. **Change Default Credentials**
   - Update `SECRET_KEY` environment variable
   - Change RabbitMQ `guest/guest` credentials
   - Use HTTPS instead of HTTP

2. **Database**
   - Back up `allocare.db` regularly
   - Use proper file permissions (chmod 600)
   - Consider migrating to PostgreSQL

3. **RabbitMQ**
   - Change default user/password
   - Disable RabbitMQ management console on production
   - Use TLS/SSL for connections

4. **Flask**
   - Disable debug mode: `debug=False`
   - Use production WSGI server (gunicorn/uWSGI)
   - Implement rate limiting
   - Add CSRF protection

## Quick Command Reference

### Essential Commands

```powershell
# Start entire system (recommended)
docker compose up --build

# Start in background
docker compose up --build -d

# Stop all services
docker compose down

# Check service status
docker compose ps

# View logs
docker compose logs allocare-web      # Flask app logs
docker compose logs allocare-consumer # Consumer logs
docker compose logs allocare-rabbitmq # RabbitMQ logs

# View last N lines of logs
docker compose logs --tail 20 allocare-web

# Follow logs in real-time
docker compose logs -f allocare-consumer

# Restart specific service
docker compose restart allocare-web

# Rebuild after code changes
docker compose up --build

# Remove all containers and data
docker compose down -v
```

### Database Commands

```powershell
# Connect to database
sqlite3 data/allocare.db

# View all tables
sqlite3 data/allocare.db ".schema"

# Count records
sqlite3 data/allocare.db "SELECT COUNT(*) FROM consumed_events;"

# View recent events
sqlite3 data/allocare.db "SELECT * FROM consumed_events ORDER BY id DESC LIMIT 5;"

# View alerts
sqlite3 data/allocare.db "SELECT alert_type, severity FROM alerts;"

# Reset database (delete all data)
Remove-Item data/allocare.db -Force
```

### Clear Cache/Reset

```powershell
# Clear Python cache
Get-ChildItem -Path __pycache__ -Recurse -Force | Remove-Item -Recurse -Force

# Clear Docker cache and rebuild
docker compose down
docker image prune -a
docker compose up --build

# Hard refresh browser
# Windows: Ctrl+Shift+Delete then Ctrl+Shift+R
# Mac: Cmd+Shift+Delete then Cmd+Shift+R
```

### Local Development (Without Docker)

```powershell
# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Start Flask app
python app.py

# Start consumer service
python consumer.py

# Install dependencies
pip install -r requirements.txt

# Exit virtual environment
deactivate
```

## Contributing

When adding new features:

1. **New Publisher Event Type**
   - Add route in app.py
   - Create RabbitMQ queue
   - Update QUEUES list in consumer.py
   - Add alert generation logic if critical

2. **New Subscriber Feature**
   - Add route in `/subscriber-*` section
   - Create template
   - Add CSS styling
   - Update sidebar navigation

3. **Database Schema Change**
   - Modify init_db() function
   - Create migration if needed
   - Update consumer logic if applicable

## Project URLs

Once system is running:

| Component | URL |
|-----------|-----|
| Publisher App | http://localhost:5000 |
| Subscriber Login | http://localhost:5000/subscriber-login |
| RabbitMQ Admin | http://localhost:15672 |
| API Health Check | http://localhost:5000/health |

## License

Internal use only - Hospital Resource Allocation System

## Support

For issues or questions:
1. Check SUBSCRIBER_SYSTEM.md for detailed architecture documentation
2. Review VERIFICATION_GUIDE.md for step-by-step testing guide
3. Check RabbitMQ management console for queue status: http://localhost:15672
4. View consumer logs: `docker compose logs allocare-consumer`
5. Verify database schema: `sqlite3 data/allocare.db ".schema"`
6. Common issues? See **Common Issues & Solutions** section above

## Quick Start Summary

**TL;DR - Get running in 2 minutes:**

```powershell
cd c:\User\Documents\Allocare
docker compose up --build
```

Then visit:
- Publisher: http://localhost:5000
- Subscriber: http://localhost:5000/subscriber-login
- RabbitMQ: http://localhost:15672

---