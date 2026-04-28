# 🏭 Clean Room EMS - Environmental Monitoring System

A comprehensive IoT monitoring system for **Sterile GMP Cleanroom Facilities** with MQTT support, cloud backup, cybersecurity features, and integrated dashboard. Complete solution for monitoring temperature, humidity, differential pressure, and particle count with automated alerting and data storage.

**🔒 GMP-Compliant & Resilient**: Designed for continuous compliance even during system failures. See [GMP_COMPLIANCE.md](GMP_COMPLIANCE.md) for detailed compliance framework.

## ✨ Complete Features

### Core Monitoring
- **Real-time Sensor Monitoring**: Temperature, Humidity, Differential Pressure, Particle Count
- **MQTT Integration**: Full MQTT support for IoT sensor data ingestion
- **Multi-Room Support**: Monitor multiple rooms and devices simultaneously
- **Local DDU Interfaces**: Digital Display Units for each room

### Data Management
- **PostgreSQL Database**: Robust data storage with optimized indexes
- **Real-time Data Ingestion**: Continuous data recording
- **Historical Data Storage**: Long-term data retention
- **CSV Data Import**: Load historical data from CSV files

### Backup System (FREE Local Backups by Default!)
- **Automated Scheduled Backups**: Daily/hourly/weekly backup options
- **Encrypted Backups**: Secure backup encryption (FREE)
- **Local Storage**: FREE backups to local disk (default, recommended)
- **Cloud Storage Support**: Optional AWS S3/Azure (paid) - not required!
- **Backup Integrity**: SHA256 verification
- **Retention Management**: Configurable backup retention
- **No Cloud Account Needed**: Works perfectly with FREE local backups!

### Security & Integrity
- **Data Encryption**: Encryption at rest and in transit
- **Integrity Checks**: SHA256 hashing for data verification
- **Anomaly Detection**: Real-time threat detection
- **Access Control**: Authentication and authorization
- **Audit Logging**: Complete security audit trail
- **Malware Protection**: Input validation and sanitization

### Alerting System
- **Temperature Alerts**: Configurable threshold alerts
- **Humidity Alerts**: Real-time humidity monitoring
- **Differential Pressure Alerts**: Pressure threshold monitoring
- **Particle Count Alerts**: Particle limit alerts
- **Security Alerts**: Cyber attack detection
- **Visual Indicators**: Real-time alert notifications

### Dashboard & Interfaces
- **Integrated Dashboard**: Real-time web dashboard
- **Live Charts**: Trend visualization with Chart.js
- **WebSocket Updates**: Real-time data streaming
- **RESTful API**: Complete API for data access
- **DDU Interfaces**: Local display units per room
- **Responsive Design**: Works on all devices

## 🏗️ Architecture

```
┌─────────────┐         ┌─────────────┐         ┌──────────┐
│   CSV Files │ ─Load─> │ PostgreSQL  │ <─Poll─ │ Backend  │
│             │         │  Database   │         │ (FastAPI)│
└─────────────┘         └─────────────┘         └──────────┘
                                                       │
                        ┌──────────────────────────────┼──────────────┐
                        │                              │              │
                        ▼                              ▼              ▼
                  ┌──────────┐                 ┌──────────┐  ┌──────────┐
                  │  API     │                 │Dashboard │  │ WebSocket│
                  │ Endpoints│                 │(Frontend)│  │   Clients│
                  └──────────┘                 └──────────┘  └──────────┘
```

## 📋 Prerequisites

- Python 3.8+
- PostgreSQL 12+

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup PostgreSQL Database

```bash
# Create database
psql -U postgres -f "CREATE DATABASE cleanroom_db.sql"

# Or manually:
psql -U postgres
CREATE DATABASE cleanroom_db;
```

### 3. Configure Environment Variables

Copy `env.example` to `.env` and update values:

```bash
cp env.example .env
```

Edit `.env`:
```env
DATABASE_URL=postgresql+asyncpg://postgres:YOUR_PASSWORD@localhost:5432/cleanroom_db
POLL_INTERVAL=2.0

ALERT_TEMP_MIN=18
ALERT_TEMP_MAX=25
ALERT_HUMIDITY_MIN=40
ALERT_HUMIDITY_MAX=60
ALERT_PRESSURE_MIN=8
ALERT_PRESSURE_MAX=20
ALERT_PARTICLE_LIMIT=300
```

### 4. Load CSV Data into Database

```bash
python load_csv_data.py
```

This will load `cleanroom_synthetic.csv` and `cleanroom_team.csv` into your PostgreSQL database.

### 5. Start the Backend

```bash
python main.py
```

Or using uvicorn directly:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`
API docs at `http://localhost:8000/docs`

### 6. Open the Dashboard

Open your browser and navigate to:
```
http://localhost:8000/
```

Or directly open `frontend/index.html` in your browser.

## 📡 API Endpoints

### Health Check
```http
GET /health
```
Returns system status and database connection info.

### Get All Logs
```http
GET /logs
```
Returns the last 200 sensor readings.

### Get Alerts
```http
GET /alerts
```
Returns all readings that triggered alerts.

### Get Statistics
```http
GET /stats
```
Returns summary statistics (averages, counts, etc.)

### WebSocket
```http
WS /ws/cleanroom
```
Real-time sensor data stream (polls database every 2 seconds by default).

## 🎛️ Configuration

### Polling Interval

Edit `.env` to change how often the system polls the database:
```env
POLL_INTERVAL=2.0  # seconds
```

### Alert Thresholds

Edit `.env` to customize alert thresholds:

- `ALERT_TEMP_MIN/MAX`: Temperature range (°C)
- `ALERT_HUMIDITY_MIN/MAX`: Humidity range (%)
- `ALERT_PRESSURE_MIN/MAX`: Pressure range (Pa)
- `ALERT_PARTICLE_LIMIT`: Maximum particle count

## 📊 Dashboard Features

- **Real-time Metrics**: Live updates of all sensor values
- **Trend Charts**: Visual representation of sensor trends
- **Alert Notifications**: Visual alerts for threshold violations
- **Security Status**: Real-time monitoring of cyber attack status
- **Recent Logs**: Tabular view of recent sensor readings
- **Responsive Design**: Works on desktop, tablet, and mobile

## 🛠️ Development

### Project Structure

```
iot-demo/
├── main.py                 # FastAPI backend server
├── load_csv_data.py        # CSV to PostgreSQL loader
├── requirements.txt        # Python dependencies
├── .env                    # Environment variables (create from env.example)
├── frontend/
│   └── index.html         # Dashboard frontend
├── backend/               # Modular backend components (optional)
│   ├── config.py
│   ├── database.py
│   └── alert_engine.py
├── cleanroom_synthetic.csv # Sample sensor data
├── cleanroom_team.csv      # Sample sensor data
└── CREATE DATABASE cleanroom_db.sql  # Database schema
```

### Adding New Data

1. **Add CSV file**: Place CSV files with the same structure as the existing ones
2. **Load data**: Run `python load_csv_data.py` (it will load all CSV files)
3. **View data**: Refresh the dashboard to see new data

### Database Schema

The `cleanroom_logs` table structure:
```sql
- id: SERIAL PRIMARY KEY
- particles: INT
- temperature: FLOAT
- humidity: FLOAT
- pressure: FLOAT
- door_status: TEXT
- user_id: TEXT
- attack_status: TEXT
- timestamp: TIMESTAMP
```

## 🔒 Security Considerations

1. **Environment Variables**: Never commit `.env` to version control
2. **Database**: Use strong passwords and restrict access
3. **CORS**: Update CORS settings for production
4. **WebSocket**: Add authentication for WebSocket connections in production

## 🐛 Troubleshooting

### Database Connection Issues
- Verify PostgreSQL is running: `psql -U postgres -l`
- Check DATABASE_URL in `.env`
- Ensure database exists: `psql -U postgres -c "SELECT 1 FROM pg_database WHERE datname='cleanroom_db';"`

### No Data Showing
- Verify data was loaded: `python load_csv_data.py`
- Check database has records: `psql -U postgres -d cleanroom_db -c "SELECT COUNT(*) FROM cleanroom_logs;"`
- Check backend logs for errors

### WebSocket Issues
- Check browser console for errors
- Verify backend is running on correct port
- Ensure CORS is properly configured

## 📝 License

This project is open source and available for modification and distribution.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📞 Support

For issues and questions, please open an issue on the project repository.

---

**Made with ❤️ for IoT Cleanroom Monitoring**
