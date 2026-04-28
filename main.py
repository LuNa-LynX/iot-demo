import os
import json
import logging
from datetime import datetime, date
from typing import List, Any, Dict, Optional
import asyncio
import queue
from decimal import Decimal

import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from pydantic import BaseModel

# Import RBAC system
from rbac import (
    rbac_manager, 
    get_current_user, 
    require_permission_dep, 
    require_role_dep,
    User, 
    Role, 
    Permission
)

# Load .env config
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", 2.0))  # Poll database every N seconds
REPLAY_MODE = os.getenv("REPLAY_MODE", "true").lower() == "true"  # Replay existing data continuously
REPLAY_INTERVAL = float(os.getenv("REPLAY_INTERVAL", 5.0))  # Seconds between replay records

TEMP_MIN = float(os.getenv("ALERT_TEMP_MIN", 18))
TEMP_MAX = float(os.getenv("ALERT_TEMP_MAX", 25))
HUMIDITY_MIN = float(os.getenv("ALERT_HUMIDITY_MIN", 40))
HUMIDITY_MAX = float(os.getenv("ALERT_HUMIDITY_MAX", 60))
PRESSURE_MIN = float(os.getenv("ALERT_PRESSURE_MIN", 8))
PRESSURE_MAX = float(os.getenv("ALERT_PRESSURE_MAX", 20))
PARTICLE_LIMIT = float(os.getenv("ALERT_PARTICLE_LIMIT", 300))

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("CleanroomBackend")

# PostgreSQL connection pool for sync operations
pg_pool = pool.SimpleConnectionPool(1, 20, DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"))

def get_pg_conn():
    conn = pg_pool.getconn()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection pool error")
    return conn

def release_pg_conn(conn):
    pg_pool.putconn(conn)

# Helper function to serialize datetime and Decimal objects to JSON
def serialize_datetime(obj: Any) -> Any:
    """Convert datetime and Decimal objects to JSON-serializable formats"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)  # Convert Decimal to float for JSON
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj  # Already JSON serializable
    elif isinstance(obj, dict):
        return {key: serialize_datetime(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [serialize_datetime(item) for item in obj]
    elif hasattr(obj, '_mapping'):  # Handle Row objects from SQLAlchemy
        return {key: serialize_datetime(value) for key, value in obj._mapping.items()}
    else:
        return obj

# Async SQLAlchemy setup for API endpoints
engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

async def get_db():
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()

# Initialize DB table + indexes
async def init_db():
    async with engine.begin() as conn:
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cleanroom_logs (
            id SERIAL PRIMARY KEY,
            particles INT,
            temperature FLOAT,
            humidity FLOAT,
            pressure FLOAT,
            door_status TEXT,
            user_id TEXT,
            attack_status TEXT,
            timestamp TIMESTAMP
        );
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_time ON cleanroom_logs(timestamp DESC);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_temp ON cleanroom_logs(temperature);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_humidity ON cleanroom_logs(humidity);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_pressure ON cleanroom_logs(pressure);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_attack ON cleanroom_logs(attack_status);"))

        # Create rooms table
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            room_name VARCHAR(100) UNIQUE NOT NULL,
            description TEXT,
            created_by VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rooms_name ON rooms(room_name);"))
        
        # Create users table for authentication
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(100) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL,
            user_id VARCHAR(100),
            room_id INTEGER REFERENCES rooms(id) ON DELETE SET NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);"))
        
        # Add room_id to users table if it doesn't exist (for existing databases)
        await conn.execute(text("""
        ALTER TABLE users 
        ADD COLUMN IF NOT EXISTS room_id INTEGER REFERENCES rooms(id) ON DELETE SET NULL;
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_room ON users(room_id);"))
        
        # Add room_id to cleanroom_logs for filtering
        await conn.execute(text("""
        ALTER TABLE cleanroom_logs 
        ADD COLUMN IF NOT EXISTS room_id INTEGER REFERENCES rooms(id) ON DELETE SET NULL;
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cleanroom_room ON cleanroom_logs(room_id);"))
        
        # Create blocked_users table
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS blocked_users (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            blocked_until TIMESTAMP,
            reason TEXT,
            attack_count INT DEFAULT 1,
            is_permanent BOOLEAN DEFAULT FALSE,
            unblocked_at TIMESTAMP,
            unblocked_by VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_blocked_user_id ON blocked_users(user_id);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_blocked_until ON blocked_users(blocked_until);"))
        
        # Create security_incidents table
        await conn.execute(text("""
        CREATE TABLE IF NOT EXISTS security_incidents (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(100),
            incident_type VARCHAR(50) NOT NULL,
            attack_indicators TEXT[],
            sensor_data JSONB,
            severity VARCHAR(20) DEFAULT 'HIGH',
            status VARCHAR(20) DEFAULT 'ACTIVE',
            blocked BOOLEAN DEFAULT FALSE,
            action_taken TEXT,
            resolved_at TIMESTAMP,
            resolved_by VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_incidents_user_id ON security_incidents(user_id);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_incidents_created ON security_incidents(created_at DESC);"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_incidents_status ON security_incidents(status);"))

    logger.info("Database ready")
    
    # Initialize users in database (sync operation)
    init_users_db()
    
    # Load blocked users into cache
    load_blocked_users_cache()

def init_users_db():
    """Initialize users table and create default users if they don't exist"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Check if default users exist, if not create them
        default_users = [
            ("admin", "admin123", "admin"),
            ("operator", "operator123", "operator"),
            ("technician", "tech123", "technician"),
            ("viewer", "viewer123", "viewer"),
            ("guest", "guest123", "guest"),
        ]
        
        for username, password, role in default_users:
            cur.execute("SELECT username FROM users WHERE username = %s", (username,))
            if not cur.fetchone():
                # User doesn't exist, create it
                from rbac import rbac_manager
                hashed_password = rbac_manager.hash_password(password)
                user_id = f"user_{username}"
                cur.execute(
                    "INSERT INTO users (username, password_hash, role, user_id) VALUES (%s, %s, %s, %s)",
                    (username, hashed_password, role, user_id)
                )
                logger.info(f"Created default user in database: {username} with role: {role}")
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        logger.info("✅ Users database initialized")
    except Exception as e:
        logger.error(f"Error initializing users database: {e}")

def load_blocked_users_cache():
    """Load currently blocked users into cache"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT user_id, blocked_until, reason, attack_count, is_permanent
            FROM blocked_users
            WHERE (is_permanent = TRUE OR blocked_until > CURRENT_TIMESTAMP)
            AND unblocked_at IS NULL
        """)
        rows = cur.fetchall()
        cur.close()
        release_pg_conn(conn)
        
        from datetime import timezone
        for user_id, blocked_until, reason, attack_count, is_permanent in rows:
            # Ensure blocked_until is timezone-aware
            if blocked_until and blocked_until.tzinfo is None:
                # If timezone-naive, assume it's UTC
                blocked_until = blocked_until.replace(tzinfo=timezone.utc)
            
            blocked_users_cache[user_id] = {
                "blocked_until": blocked_until,
                "is_permanent": is_permanent,
                "reason": reason,
                "attack_count": attack_count
            }
        
        logger.info(f"✅ Loaded {len(blocked_users_cache)} blocked users into cache")
    except Exception as e:
        logger.error(f"Error loading blocked users cache: {e}")

async def auto_unblock_worker():
    """Background task to auto-unblock users after cooldown period"""
    while True:
        try:
            await asyncio.sleep(60)  # Check every minute
            
            conn = get_pg_conn()
            cur = conn.cursor()
            
            # Find users whose block has expired
            cur.execute("""
                SELECT user_id 
                FROM blocked_users 
                WHERE blocked_until IS NOT NULL 
                AND blocked_until <= CURRENT_TIMESTAMP
                AND is_permanent = FALSE
                AND unblocked_at IS NULL
            """)
            expired_blocks = cur.fetchall()
            
            if expired_blocks:
                for (user_id,) in expired_blocks:
                    cur.execute("""
                        UPDATE blocked_users 
                        SET unblocked_at = CURRENT_TIMESTAMP,
                            unblocked_by = 'AUTO_UNBLOCK'
                        WHERE user_id = %s AND unblocked_at IS NULL
                    """, (user_id,))
                    
                    # Remove from cache
                    if user_id in blocked_users_cache:
                        del blocked_users_cache[user_id]
                    
                    logger.info(f"✅ Auto-unblocked user: {user_id} (cooldown expired)")
                
                conn.commit()
            
            cur.close()
            release_pg_conn(conn)
        except Exception as e:
            logger.error(f"Error in auto-unblock worker: {e}")
            try:
                release_pg_conn(conn)
            except:
                pass

# Active WebSocket connections
ws_clients: List[WebSocket] = []

# Store WebSocket connections with user info for room filtering
ws_client_info = {}  # {ws: {user, room_id}}

# Queue for WebSocket broadcasts
ws_queue = queue.Queue()

# Track last processed record ID
last_processed_id = 0

# Track recent sensor readings for pattern detection
recent_readings = []  # Store last 10 readings for anomaly detection
MAX_RECENT_READINGS = 10

# Track blocked users (in-memory cache)
blocked_users_cache = {}  # {user_id: {blocked_until, reason, attack_count, is_permanent}}

# Track attack frequency per user
user_attack_count = {}  # {user_id: count}

# Blocking configuration
BLOCK_COOLDOWN_MINUTES = 60  # Auto-unblock after 1 hour (gives admin time to review and take action)
PERMANENT_BLOCK_THRESHOLD = 5  # Permanent block after 5 attacks

def detect_cyber_attack(particles: int, temperature: float, humidity: float, 
                       pressure: float, door_status: str, user_id: str) -> tuple[str, list[str]]:
    """
    Detect cyber attacks based on multiple indicators:
    1. Impossible sensor values (out of physical bounds)
    2. Rapid simultaneous changes in multiple sensors
    3. Unusual patterns (e.g., door open with high particles)
    4. Multiple simultaneous alerts
    5. Suspicious user activity patterns
    
    Returns: (attack_status, attack_indicators)
    """
    attack_indicators = []
    
    # 1. Check for impossible values (strong indicator of data injection)
    if temperature < -50 or temperature > 100:
        attack_indicators.append("IMPOSSIBLE_TEMPERATURE")
    
    if humidity < 0 or humidity > 100:
        attack_indicators.append("IMPOSSIBLE_HUMIDITY")
    
    if pressure < 0 or pressure > 1000:
        attack_indicators.append("IMPOSSIBLE_PRESSURE")
    
    if particles < 0 or particles > 10000:
        attack_indicators.append("IMPOSSIBLE_PARTICLES")
    
    # 2. Check for multiple simultaneous critical alerts (coordinated attack pattern)
    critical_alerts = 0
    if not (TEMP_MIN <= temperature <= TEMP_MAX):
        critical_alerts += 1
    if not (HUMIDITY_MIN <= humidity <= HUMIDITY_MAX):
        critical_alerts += 1
    if not (PRESSURE_MIN <= pressure <= PRESSURE_MAX):
        critical_alerts += 1
    if particles > PARTICLE_LIMIT:
        critical_alerts += 1
    
    # If 3+ sensors are in critical state simultaneously, likely an attack
    if critical_alerts >= 3:
        attack_indicators.append("MULTIPLE_CRITICAL_ALERTS")
    
    # 3. Check for suspicious patterns
    # Door open with extremely high particles (contamination scenario)
    if door_status == "OPEN" and particles > PARTICLE_LIMIT * 1.5:
        attack_indicators.append("DOOR_OPEN_HIGH_PARTICLES")
    
    # Extreme temperature with door open (environmental attack)
    if door_status == "OPEN" and (temperature < TEMP_MIN - 5 or temperature > TEMP_MAX + 5):
        attack_indicators.append("DOOR_OPEN_EXTREME_TEMP")
    
    # 4. Analyze recent readings for rapid changes (data injection pattern)
    if len(recent_readings) >= 3:
        # Check for rapid simultaneous changes
        last_reading = recent_readings[-1]
        temp_change = abs(temperature - last_reading.get("temperature", temperature))
        hum_change = abs(humidity - last_reading.get("humidity", humidity))
        press_change = abs(pressure - last_reading.get("pressure", pressure))
        
        # If all sensors change dramatically at once (unlikely in real scenario)
        if temp_change > 5 and hum_change > 10 and press_change > 5:
            attack_indicators.append("RAPID_MULTI_SENSOR_CHANGE")
        
        # Check for oscillating pattern (sensor manipulation)
        if len(recent_readings) >= 5:
            temps = [r.get("temperature", 0) for r in recent_readings[-5:]]
            if max(temps) - min(temps) > 10 and all(abs(temps[i] - temps[i+1]) > 3 for i in range(4)):
                attack_indicators.append("OSCILLATING_PATTERN")
    
    # 5. Check for suspicious user patterns (if same user triggers multiple alerts)
    if len(recent_readings) >= 5:
        recent_users = [r.get("user_id", "") for r in recent_readings[-5:]]
        if recent_users.count(user_id) >= 4 and critical_alerts >= 2:
            attack_indicators.append("SUSPICIOUS_USER_PATTERN")
    
    # If we have 2+ attack indicators, classify as cyber attack
    if len(attack_indicators) >= 2:
        logger.warning(f"🛡 CYBER ATTACK DETECTED! Indicators: {', '.join(attack_indicators)}")
        return ("CYBER_ATTACK", attack_indicators)
    elif len(attack_indicators) == 1 and "IMPOSSIBLE" in attack_indicators[0]:
        # Single impossible value is also suspicious
        logger.warning(f"🛡 CYBER ATTACK DETECTED! Indicator: {attack_indicators[0]}")
        return ("CYBER_ATTACK", attack_indicators)
    else:
        return ("NORMAL", [])

def is_user_blocked(user_id: str) -> bool:
    """Check if user is currently blocked"""
    if not user_id:
        return False
    
    # Check cache first
    if user_id in blocked_users_cache:
        block_info = blocked_users_cache[user_id]
        if block_info.get("is_permanent"):
            return True
        if block_info.get("blocked_until"):
            from datetime import datetime, timezone
            blocked_until = block_info["blocked_until"]
            
            # Ensure blocked_until is timezone-aware
            if blocked_until and blocked_until.tzinfo is None:
                blocked_until = blocked_until.replace(tzinfo=timezone.utc)
            
            if datetime.now(timezone.utc) < blocked_until:
                return True
            else:
                # Block expired, remove from cache
                del blocked_users_cache[user_id]
                return False
        return True
    
    # Check database
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT blocked_until, is_permanent 
            FROM blocked_users 
            WHERE user_id = %s 
            AND (is_permanent = TRUE OR blocked_until > CURRENT_TIMESTAMP)
            AND unblocked_at IS NULL
            ORDER BY blocked_at DESC
            LIMIT 1
        """, (user_id,))
        row = cur.fetchone()
        cur.close()
        release_pg_conn(conn)
        
        if row:
            blocked_until, is_permanent = row
            # Ensure blocked_until is timezone-aware
            from datetime import timezone
            if blocked_until and blocked_until.tzinfo is None:
                # If timezone-naive, assume it's UTC
                blocked_until = blocked_until.replace(tzinfo=timezone.utc)
            
            blocked_users_cache[user_id] = {
                "blocked_until": blocked_until,
                "is_permanent": is_permanent,
                "reason": "Multiple cyber attacks detected"
            }
            return True
        return False
    except Exception as e:
        logger.error(f"Error checking blocked user: {e}")
        return False

def block_user(user_id: str, attack_indicators: list, sensor_data: dict) -> dict:
    """Block a user and create security incident"""
    if not user_id:
        return {"blocked": False, "reason": "No user_id provided"}
    
    # Track attack count
    user_attack_count[user_id] = user_attack_count.get(user_id, 0) + 1
    attack_count = user_attack_count[user_id]
    
    # Determine block duration
    from datetime import datetime, timedelta, timezone
    is_permanent = attack_count >= PERMANENT_BLOCK_THRESHOLD
    blocked_until = None if is_permanent else datetime.now(timezone.utc) + timedelta(minutes=BLOCK_COOLDOWN_MINUTES)
    
    reason = f"Cyber attack detected: {', '.join(attack_indicators)}"
    if is_permanent:
        reason += f" (Permanent block after {attack_count} attacks)"
    else:
        reason += f" (Temporary block for 1 hour - Attack #{attack_count})"
    
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Insert or update blocked_users
        cur.execute("""
            INSERT INTO blocked_users (user_id, blocked_until, reason, attack_count, is_permanent)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (user_id, blocked_until, reason, attack_count, is_permanent))
        
        # If user already blocked, update attack count
        if cur.rowcount == 0:
            cur.execute("""
                UPDATE blocked_users 
                SET attack_count = attack_count + 1,
                    blocked_until = COALESCE(blocked_until, %s),
                    is_permanent = CASE WHEN attack_count + 1 >= %s THEN TRUE ELSE is_permanent END
                WHERE user_id = %s AND unblocked_at IS NULL
            """, (blocked_until, PERMANENT_BLOCK_THRESHOLD, user_id))
        
        # Create security incident
        cur.execute("""
            INSERT INTO security_incidents 
            (user_id, incident_type, attack_indicators, sensor_data, severity, status, blocked, action_taken)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id,
            "CYBER_ATTACK",
            attack_indicators,
            json.dumps(sensor_data),
            "HIGH",
            "ACTIVE",
            True,
            f"User blocked - {reason}"
        ))
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        # Update cache
        blocked_users_cache[user_id] = {
            "blocked_until": blocked_until,
            "is_permanent": is_permanent,
            "reason": reason,
            "attack_count": attack_count
        }
        
        logger.error(f"🚫 USER BLOCKED: {user_id} - {reason}")
        
        return {
            "blocked": True,
            "user_id": user_id,
            "attack_count": attack_count,
            "is_permanent": is_permanent,
            "blocked_until": blocked_until.isoformat() if blocked_until else None,
            "reason": reason,
            "indicators": attack_indicators
        }
    except Exception as e:
        logger.error(f"Error blocking user: {e}")
        return {"blocked": False, "error": str(e)}

def trigger_emergency_protocols(user_id: str, attack_indicators: list):
    """Trigger emergency protocols when attack detected"""
    logger.warning(f"🚨 EMERGENCY PROTOCOLS TRIGGERED for user {user_id}")
    logger.warning(f"   Attack indicators: {', '.join(attack_indicators)}")
    logger.warning(f"   Actions: Door closure recommended, Alarm activated, System isolated")
    
    # In a real system, this would:
    # - Send commands to close cleanroom doors
    # - Activate physical alarms
    # - Isolate affected sensors
    # - Notify security team
    # - Trigger backup systems

async def ws_broadcast(message: str, room_id: Optional[int] = None):
    """Broadcast message to WebSocket clients, filtered by room if specified"""
    dead = []
    message_data = json.loads(message) if isinstance(message, str) else message
    broadcast_room_id = room_id or message_data.get("room_id")
    
    for ws in ws_clients:
        try:
            # Get client info
            client_info = ws_client_info.get(ws, {})
            client_user = client_info.get("user")
            client_room_id = client_info.get("room_id")
            
            # Filter by room: if broadcast has room_id, only send to matching clients
            # Admin gets all messages, users only get their room's messages
            if broadcast_room_id is not None:
                if client_user and client_user.role == Role.ADMIN:
                    # Admin sees all
                    await ws.send_text(message if isinstance(message, str) else json.dumps(message))
                elif client_room_id == broadcast_room_id:
                    # User sees only their room
                    await ws.send_text(message if isinstance(message, str) else json.dumps(message))
                # Otherwise skip this client
            else:
                # No room filter, send to all
                await ws.send_text(message if isinstance(message, str) else json.dumps(message))
        except:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)
        ws_client_info.pop(ws, None)

# Background task to poll database for new records
async def database_poller():
    """Poll database for new records and broadcast via WebSocket"""
    global last_processed_id
    
    # Initialize last_processed_id
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        
        if REPLAY_MODE:
            # In replay mode, start from the beginning
            cur.execute("SELECT COALESCE(MIN(id), 0) FROM cleanroom_logs;")
            last_processed_id = cur.fetchone()[0] or 0
            logger.info(f"🔄 REPLAY MODE: Starting from record ID {last_processed_id}")
            logger.info(f"📊 Replaying existing data every {REPLAY_INTERVAL} seconds...")
        else:
            # Normal mode: start from latest record
            cur.execute("SELECT COALESCE(MAX(id), 0) FROM cleanroom_logs;")
            last_processed_id = cur.fetchone()[0] or 0
            logger.info(f"✅ Starting database poller from record ID {last_processed_id}")
            logger.info(f"📊 Polling every {POLL_INTERVAL} seconds for new records...")
        
        cur.close()
        release_pg_conn(conn)
    except Exception as e:
        logger.error(f"Error initializing database poller: {e}")
        last_processed_id = 0
    
    while True:
        try:
            conn = get_pg_conn()
            cur = conn.cursor()
            
            if REPLAY_MODE:
                # Replay mode: process records one at a time in sequence
                cur.execute("""
                    SELECT id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, COALESCE(room_id, NULL) as room_id
                    FROM cleanroom_logs
                    WHERE id >= %s
                    ORDER BY id ASC
                    LIMIT 1;
                """, (last_processed_id + 1,))
            else:
                # Normal mode: get all new records since last poll
                cur.execute("""
                    SELECT id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, COALESCE(room_id, NULL) as room_id
                    FROM cleanroom_logs
                    WHERE id > %s
                    ORDER BY id ASC
                    LIMIT 50;
                """, (last_processed_id,))
            
            rows = cur.fetchall()
            
            if rows:
                logger.info(f"📥 Found {len(rows)} new records")
                
                for row in rows:
                    record_id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, room_id = row
                    
                    # Update last processed ID
                    last_processed_id = record_id
                    
                    # Format data
                    temp = float(temperature)
                    hum = float(humidity)
                    press = float(pressure)
                    parts = particles
                    
                    # Check if user is already blocked - skip processing if blocked
                    if is_user_blocked(user_id):
                        logger.warning(f"⛔ Skipping data from blocked user: {user_id}")
                        last_processed_id = record_id
                        continue
                    
                    # Detect cyber attack using pattern analysis
                    detected_attack_status, attack_indicators = detect_cyber_attack(parts, temp, hum, press, door_status, user_id)
                    
                    # If attack detected, block user and trigger emergency protocols
                    block_info = None
                    if detected_attack_status == "CYBER_ATTACK":
                        # Prepare sensor data for incident logging
                        sensor_data = {
                            "particles": parts,
                            "temperature": temp,
                            "humidity": hum,
                            "pressure": press,
                            "door_status": door_status,
                            "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp)
                        }
                        
                        # Block the user
                        block_info = block_user(user_id, attack_indicators, sensor_data)
                        
                        # Trigger emergency protocols
                        trigger_emergency_protocols(user_id, attack_indicators)
                    
                    # Update attack_status in database if detection differs (only if original was NORMAL)
                    if attack_status == "NORMAL" and detected_attack_status == "CYBER_ATTACK":
                        try:
                            update_conn = get_pg_conn()
                            update_cur = update_conn.cursor()
                            update_cur.execute(
                                "UPDATE cleanroom_logs SET attack_status = %s WHERE id = %s",
                                (detected_attack_status, record_id)
                            )
                            update_conn.commit()
                            update_cur.close()
                            release_pg_conn(update_conn)
                            attack_status = detected_attack_status  # Use detected status
                            logger.info(f"🛡 Updated record {record_id}: Attack detected and status updated")
                        except Exception as e:
                            logger.error(f"Error updating attack status: {e}")
                    
                    # Use detected status if it's an attack, otherwise use original
                    final_attack_status = detected_attack_status if detected_attack_status == "CYBER_ATTACK" else attack_status
                    final_attack_indicators = attack_indicators if detected_attack_status == "CYBER_ATTACK" else []
                    
                    # Store reading for pattern analysis (keep last N readings)
                    recent_readings.append({
                        "particles": parts,
                        "temperature": temp,
                        "humidity": hum,
                        "pressure": press,
                        "door_status": door_status,
                        "user_id": user_id
                    })
                    if len(recent_readings) > MAX_RECENT_READINGS:
                        recent_readings.pop(0)
                    
                    # Check for alerts and add alert flags
                    temp_alert = not (TEMP_MIN <= temp <= TEMP_MAX)
                    hum_alert = not (HUMIDITY_MIN <= hum <= HUMIDITY_MAX)
                    press_alert = not (PRESSURE_MIN <= press <= PRESSURE_MAX)
                    particle_alert = parts > PARTICLE_LIMIT
                    security_alert = final_attack_status == "CYBER_ATTACK"
                    has_alert = temp_alert or hum_alert or press_alert or particle_alert or security_alert
                    
                    # Initialize block_info if not set
                    if 'block_info' not in locals():
                        block_info = None
                    
                    data = {
                        "id": record_id,  # Include record ID for fetching details
                        "particles": parts,
                        "temperature": temp,
                        "humidity": hum,
                        "pressure": press,
                        "door_status": door_status,
                        "user_id": user_id,
                        "room_id": room_id,  # Include room_id for filtering
                        "attack_status": final_attack_status,
                        "attack_indicators": final_attack_indicators,
                        "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
                        "blocked": block_info if block_info else None,  # Include block information
                        # Alert flags for frontend
                        "alerts": {
                            "temperature": temp_alert,
                            "humidity": hum_alert,
                            "pressure": press_alert,
                            "particles": particle_alert,
                            "security": security_alert,
                            "has_alert": has_alert
                        }
                    }
                    
                    # Log alerts
                    if temp_alert:
                        logger.warning(f"🚨 ALERT: Temperature {temp}°C out of range [{TEMP_MIN}-{TEMP_MAX}]!")
                    
                    if hum_alert:
                        logger.warning(f"🚨 ALERT: Humidity {hum}% out of range [{HUMIDITY_MIN}-{HUMIDITY_MAX}]!")
                    
                    if press_alert:
                        logger.warning(f"🚨 ALERT: Pressure {press}Pa out of range [{PRESSURE_MIN}-{PRESSURE_MAX}]!")
                    
                    if security_alert:
                        logger.error(f"🛡 CYBER ATTACK DETECTED! (attack: {final_attack_status}, particles: {parts})")
                    elif particle_alert:
                        logger.warning(f"⚠️ HIGH PARTICLE ALERT: Particles {parts} exceed limit {PARTICLE_LIMIT}")
                    
                    # Queue WebSocket broadcast with room_id
                    try:
                        ws_queue.put_nowait((json.dumps(data), room_id))
                    except Exception as e:
                        logger.error(f"WebSocket queue error: {e}")
            
            cur.close()
            release_pg_conn(conn)
            
            # In replay mode, wait between records; in normal mode, wait for next poll
            sleep_time = REPLAY_INTERVAL if REPLAY_MODE else POLL_INTERVAL
            await asyncio.sleep(sleep_time)
            
        except Exception as e:
            logger.error(f"Database poller error: {e}")
            try:
                release_pg_conn(conn)
            except:
                pass
            
            await asyncio.sleep(POLL_INTERVAL)

# Background task to process WebSocket broadcasts
async def ws_broadcast_worker():
    """Process WebSocket broadcasts from the queue"""
    while True:
        try:
            try:
                queue_item = ws_queue.get_nowait()
                if isinstance(queue_item, tuple):
                    message, room_id = queue_item
                    await ws_broadcast(message, room_id)
                else:
                    # Backward compatibility
                    await ws_broadcast(queue_item)
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
        except Exception as e:
            logger.error(f"WebSocket broadcast worker error: {e}")
            await asyncio.sleep(0.1)

# Pydantic models for API requests
class LoginRequest(BaseModel):
    username: str
    password: str

class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: Optional[str] = None  # Optional for public registration
    room_id: Optional[int] = None  # Room assignment for users

class UpdateUserRoleRequest(BaseModel):
    username: str
    role: str

# API Routes
app = FastAPI(title="Cleanroom IoT Real-Time API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve favicon - defined before static mount
@app.get("/favicon.ico")
async def favicon():
    from fastapi.responses import FileResponse
    logo_path = "frontend/nanoguard-logo.svg"
    if os.path.exists(logo_path):
        return FileResponse(logo_path, media_type="image/svg+xml")
    else:
        from fastapi.responses import Response
        return Response(status_code=404)

# Serve frontend static files
if os.path.exists("frontend"):
    try:
        app.mount("/static", StaticFiles(directory="frontend"), name="static")
        
        # Root route - redirect to login page
        @app.get("/")
        async def read_root():
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url="/login")
        
        # Serve login page (default landing page)
        @app.get("/login")
        async def login_page():
            from fastapi.responses import FileResponse
            if os.path.exists("frontend/login.html"):
                return FileResponse("frontend/login.html")
            else:
                raise HTTPException(status_code=404, detail="Login page not found")
        
        # Serve registration page
        @app.get("/register")
        async def register_page():
            from fastapi.responses import FileResponse
            if os.path.exists("frontend/register.html"):
                return FileResponse("frontend/register.html")
            else:
                raise HTTPException(status_code=404, detail="Registration page not found")
        
        # Serve main dashboard (requires authentication - checked on frontend)
        @app.get("/dashboard")
        async def dashboard():
            from fastapi.responses import FileResponse
            return FileResponse("frontend/index.html")
        
        # Serve DDU interface
        @app.get("/ddu")
        async def ddu_interface():
            from fastapi.responses import FileResponse
            if os.path.exists("ddu_interface.html"):
                return FileResponse("ddu_interface.html")
            else:
                raise HTTPException(status_code=404, detail="DDU interface not found")
    except Exception as e:
        logger.warning(f"Frontend serving error: {e}")
else:
    logger.warning("Frontend directory not found - dashboard not available")

@app.on_event("startup")
async def startup_event():
    """Initialize database and start background tasks"""
    try:
        await init_db()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.warning(f"DB init warning (may already exist): {e}")
    
    # Start background tasks
    asyncio.create_task(database_poller())
    logger.info("✅ Database poller started")
    
    asyncio.create_task(ws_broadcast_worker())
    logger.info("✅ WebSocket broadcast worker started")
    
    asyncio.create_task(auto_unblock_worker())
    logger.info("✅ Auto-unblock worker started")

# Authentication endpoints
@app.post("/api/auth/login")
async def login(credentials: LoginRequest):
    """Authenticate user and return JWT token"""
    user = rbac_manager.authenticate_user(credentials.username, credentials.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password"
        )
    
    token = rbac_manager.generate_token(user)
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": user.to_dict()
    }

@app.post("/api/auth/register")
async def register(user_data: CreateUserRequest):
    """Public registration endpoint - allows user or admin role selection"""
    try:
        if not user_data.role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Role is required. Please select User or Admin."
            )
        
        # Map "user" to VIEWER role, "admin" to ADMIN role
        if user_data.role.lower() == "user":
            target_role = Role.VIEWER
            # Users must have a room assigned
            if not user_data.room_id:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Room selection is required for user accounts"
                )
            # Verify room exists
            conn = get_pg_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT id FROM rooms WHERE id = %s", (user_data.room_id,))
                if not cur.fetchone():
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Selected room does not exist"
                    )
                cur.close()
            finally:
                release_pg_conn(conn)
        elif user_data.role.lower() == "admin":
            target_role = Role.ADMIN
            # Admins don't need room assignment
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid role. Only 'user' or 'admin' roles are allowed."
            )
        
        # Create user with room assignment if provided
        user = rbac_manager.create_user(user_data.username, user_data.password, target_role, room_id=user_data.room_id)
        token = rbac_manager.generate_token(user)
        return {
            "message": "User registered successfully",
            "access_token": token,
            "token_type": "bearer",
            "user": user.to_dict()
        }
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@app.get("/api/auth/me")
async def get_current_user_info(current_user: User = Depends(get_current_user)):
    """Get current authenticated user information"""
    user_dict = current_user.to_dict()
    # Add room information if available
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT u.room_id, r.room_name 
            FROM users u 
            LEFT JOIN rooms r ON u.room_id = r.id 
            WHERE u.username = %s
        """, (current_user.username,))
        row = cur.fetchone()
        cur.close()
        release_pg_conn(conn)
        if row and row[0]:
            user_dict['room_id'] = row[0]
            user_dict['room_name'] = row[1]
    except Exception as e:
        logger.error(f"Error fetching user room info: {e}")
    return user_dict

# Room Management Endpoints
class CreateRoomRequest(BaseModel):
    room_name: str
    description: Optional[str] = None

@app.get("/api/rooms")
async def list_rooms(current_user: User = Depends(get_current_user)):
    """List all rooms (accessible to all authenticated users for registration)"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, room_name, description, created_at FROM rooms ORDER BY room_name")
        rows = cur.fetchall()
        cur.close()
        release_pg_conn(conn)
        
        rooms = []
        for row in rows:
            rooms.append({
                "id": row[0],
                "room_name": row[1],
                "description": row[2],
                "created_at": row[3].isoformat() if row[3] else None
            })
        return rooms
    except Exception as e:
        logger.error(f"Error fetching rooms: {e}")
        raise HTTPException(status_code=500, detail="Error fetching rooms")

@app.post("/api/rooms")
async def create_room(
    room_data: CreateRoomRequest,
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Create a new room (Admin only)"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO rooms (room_name, description, created_by) VALUES (%s, %s, %s) RETURNING id, room_name, description",
            (room_data.room_name, room_data.description, current_user.username)
        )
        row = cur.fetchone()
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        return {
            "id": row[0],
            "room_name": row[1],
            "description": row[2],
            "message": f"Room '{room_data.room_name}' created successfully"
        }
    except psycopg2.IntegrityError:
        try:
            release_pg_conn(conn)
        except:
            pass
        raise HTTPException(status_code=400, detail=f"Room '{room_data.room_name}' already exists")
    except Exception as e:
        logger.error(f"Error creating room: {e}")
        try:
            release_pg_conn(conn)
        except:
            pass
        raise HTTPException(status_code=500, detail="Error creating room")

@app.delete("/api/rooms/{room_id}")
async def delete_room(
    room_id: int,
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Delete a room (Admin only)"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        # Check if room has users assigned
        cur.execute("SELECT COUNT(*) FROM users WHERE room_id = %s", (room_id,))
        user_count = cur.fetchone()[0]
        if user_count > 0:
            release_pg_conn(conn)
            raise HTTPException(
                status_code=400,
                detail=f"Cannot delete room: {user_count} user(s) are assigned to this room"
            )
        
        cur.execute("DELETE FROM rooms WHERE id = %s RETURNING room_name", (room_id,))
        row = cur.fetchone()
        if not row:
            release_pg_conn(conn)
            raise HTTPException(status_code=404, detail="Room not found")
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        return {"message": f"Room '{row[0]}' deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting room: {e}")
        try:
            release_pg_conn(conn)
        except:
            pass
        raise HTTPException(status_code=500, detail="Error deleting room")

# User management endpoints (Admin only)
@app.get("/api/users")
async def list_users(current_user: User = Depends(require_role_dep(Role.ADMIN))):
    """List all users (Admin only)"""
    return rbac_manager.list_users()

@app.post("/api/users")
async def create_user(
    user_data: CreateUserRequest,
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Create a new user (Admin only)"""
    try:
        if not user_data.role:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Role is required for admin user creation")
        role = Role(user_data.role)
        user = rbac_manager.create_user(user_data.username, user_data.password, role)
        return {"message": "User created successfully", "user": user.to_dict()}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@app.put("/api/users/{username}/role")
async def update_user_role(
    username: str,
    role_data: UpdateUserRoleRequest,
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Update user role (Admin only)"""
    try:
        new_role = Role(role_data.role)
        if rbac_manager.update_user_role(username, new_role):
            return {"message": f"User {username} role updated to {new_role.value}"}
        else:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@app.get("/health")
async def health_check():
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        
        # Check database connection
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cleanroom_logs;")
        count = cur.fetchone()[0]
        cur.close()
        release_pg_conn(conn)
        
        return {
            "status": "ok",
            "database_connected": True,
            "total_records": count,
            "last_processed_id": last_processed_id
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

# Helper function to get user's room_id
async def get_user_room_id(username: str) -> Optional[int]:
    """Get room_id for a user"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("SELECT room_id FROM users WHERE username = %s", (username,))
        row = cur.fetchone()
        cur.close()
        release_pg_conn(conn)
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.error(f"Error getting user room_id: {e}")
        return None

@app.get("/logs")
async def get_logs(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_DATA))
):
    try:
        # Get user's room_id if they're not admin
        room_id = None
        if current_user.role != Role.ADMIN:
            room_id = await get_user_room_id(current_user.username)
            if not room_id:
                # User has no room assigned, return empty
                return JSONResponse([])
        
        # Build query based on role
        if current_user.role == Role.ADMIN:
            # Admin sees all logs
            query = text("SELECT * FROM cleanroom_logs ORDER BY timestamp DESC LIMIT 200;")
            result = await db.execute(query)
        else:
            # Users see only their room's logs
            query = text("SELECT * FROM cleanroom_logs WHERE room_id = :room_id ORDER BY timestamp DESC LIMIT 200;")
            result = await db.execute(query.bindparams(room_id=room_id))
        
        rows = result.fetchall()
        # Serialize datetime objects to ISO format strings
        data = [serialize_datetime(row) for row in rows]
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error fetching logs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching logs: {str(e)}")

@app.get("/alerts")
async def get_alerts(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_ALERTS))
):
    try:
        # Get user's room_id if they're not admin
        room_id = None
        if current_user.role != Role.ADMIN:
            room_id = await get_user_room_id(current_user.username)
            if not room_id:
                return JSONResponse([])
        
        # Build query based on role
        if current_user.role == Role.ADMIN:
            query = text("""
            SELECT * FROM cleanroom_logs
            WHERE temperature < :tmin OR temperature > :tmax
               OR humidity < :hmin OR humidity > :hmax
               OR pressure < :pmin OR pressure > :pmax
               OR attack_status = 'CYBER_ATTACK'
               OR particles > :plimit
            ORDER BY timestamp DESC LIMIT 100
        """).bindparams(
            tmin=TEMP_MIN,
            tmax=TEMP_MAX,
            hmin=HUMIDITY_MIN,
            hmax=HUMIDITY_MAX,
            pmin=PRESSURE_MIN,
            pmax=PRESSURE_MAX,
            plimit=PARTICLE_LIMIT
        )
        else:
            query = text("""
                SELECT * FROM cleanroom_logs
                WHERE room_id = :room_id
                  AND (temperature < :tmin OR temperature > :tmax
                   OR humidity < :hmin OR humidity > :hmax
                   OR pressure < :pmin OR pressure > :pmax
                   OR attack_status = 'CYBER_ATTACK'
                   OR particles > :plimit)
                ORDER BY timestamp DESC LIMIT 100
            """).bindparams(
                room_id=room_id,
                tmin=TEMP_MIN,
                tmax=TEMP_MAX,
                hmin=HUMIDITY_MIN,
                hmax=HUMIDITY_MAX,
                pmin=PRESSURE_MIN,
                pmax=PRESSURE_MAX,
                plimit=PARTICLE_LIMIT
            )
        result = await db.execute(query)
        rows = result.fetchall()
        # Serialize datetime objects to ISO format strings
        data = [serialize_datetime(row) for row in rows]
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching alerts: {str(e)}")

@app.get("/stats")
async def get_stats(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_STATS))
):
    """Get summary statistics"""
    try:
        result = await db.execute(text("""
        SELECT 
            COUNT(*) as total_records,
            AVG(temperature) as avg_temperature,
            AVG(humidity) as avg_humidity,
            AVG(pressure) as avg_pressure,
            AVG(particles) as avg_particles,
            MAX(timestamp) as latest_timestamp
        FROM cleanroom_logs;
        """))
        row = result.fetchone()
        # Serialize datetime objects to ISO format strings
        data = serialize_datetime(row) if row else {}
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error fetching stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

@app.get("/api/security/incidents")
async def get_security_incidents(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_SECURITY_LOGS)),
    limit: int = 50
):
    """Get security incidents (Admin/Operator only)"""
    try:
        result = await db.execute(text("""
            SELECT id, user_id, incident_type, attack_indicators, sensor_data, 
                   severity, status, blocked, action_taken, created_at, resolved_at
            FROM security_incidents
            ORDER BY created_at DESC
            LIMIT :limit
        """).bindparams(limit=limit))
        rows = result.fetchall()
        data = [serialize_datetime(row) for row in rows]
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error fetching security incidents: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching incidents: {str(e)}")

@app.get("/api/security/blocked-users")
async def get_blocked_users(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_SECURITY_LOGS))
):
    """Get list of blocked users (Admin/Operator only)"""
    try:
        result = await db.execute(text("""
            SELECT user_id, blocked_at, blocked_until, reason, attack_count, 
                   is_permanent, unblocked_at, unblocked_by
            FROM blocked_users
            WHERE unblocked_at IS NULL
            ORDER BY blocked_at DESC
        """))
        rows = result.fetchall()
        data = [serialize_datetime(row) for row in rows]
        return JSONResponse(data)
    except Exception as e:
        logger.error(f"Error fetching blocked users: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching blocked users: {str(e)}")

@app.post("/api/security/unblock/{user_id}")
async def unblock_user(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Unblock a user (Admin only)"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE blocked_users 
            SET unblocked_at = CURRENT_TIMESTAMP, unblocked_by = %s
            WHERE user_id = %s AND unblocked_at IS NULL
        """, (current_user.username, user_id))
        
        if cur.rowcount == 0:
            cur.close()
            release_pg_conn(conn)
            raise HTTPException(status_code=404, detail="User not found or already unblocked")
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        # Remove from cache
        if user_id in blocked_users_cache:
            del blocked_users_cache[user_id]
        
        logger.info(f"✅ User {user_id} unblocked by {current_user.username}")
        return {"message": f"User {user_id} has been unblocked", "unblocked_by": current_user.username}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error unblocking user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error unblocking user: {str(e)}")

@app.post("/api/security/blocked-users/{user_id}/make-permanent")
async def make_block_permanent(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Make a temporary block permanent (Admin only)"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Check if user is currently blocked
        cur.execute("""
            SELECT id, is_permanent FROM blocked_users 
            WHERE user_id = %s AND unblocked_at IS NULL
        """, (user_id,))
        block_row = cur.fetchone()
        
        if not block_row:
            cur.close()
            release_pg_conn(conn)
            raise HTTPException(status_code=404, detail="User is not currently blocked")
        
        if block_row[1]:  # Already permanent
            cur.close()
            release_pg_conn(conn)
            raise HTTPException(status_code=400, detail="User is already permanently blocked")
        
        # Update to permanent
        cur.execute("""
            UPDATE blocked_users 
            SET is_permanent = TRUE, 
                blocked_until = NULL,
                reason = reason || ' (Made permanent by ' || %s || ')'
            WHERE user_id = %s AND unblocked_at IS NULL
        """, (current_user.username, user_id))
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        # Update cache
        if user_id in blocked_users_cache:
            blocked_users_cache[user_id]["is_permanent"] = True
            blocked_users_cache[user_id]["blocked_until"] = None
        
        logger.info(f"✅ User {user_id} block made permanent by {current_user.username}")
        return {"message": f"User {user_id} is now permanently blocked", "updated_by": current_user.username}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error making block permanent: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error updating block: {str(e)}")

class ManualBlockRequest(BaseModel):
    user_id: str
    is_permanent: bool = False
    reason: Optional[str] = None
    duration_minutes: Optional[int] = 60  # For temporary blocks

@app.post("/api/security/block-user")
async def manually_block_user(
    block_data: ManualBlockRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_role_dep(Role.ADMIN))
):
    """Manually block a user (Admin only) - can be temporary or permanent"""
    try:
        # Check if user is already blocked
        if is_user_blocked(block_data.user_id):
            raise HTTPException(status_code=400, detail="User is already blocked")
        
        from datetime import datetime, timedelta, timezone
        blocked_until = None if block_data.is_permanent else datetime.now(timezone.utc) + timedelta(minutes=block_data.duration_minutes or 60)
        reason = block_data.reason or f"Manually blocked by {current_user.username}"
        if not block_data.is_permanent:
            reason += f" (Temporary block for {block_data.duration_minutes or 60} minutes)"
        else:
            reason += " (Permanent block)"
        
        conn = get_pg_conn()
        cur = conn.cursor()
        
        # Insert block record
        cur.execute("""
            INSERT INTO blocked_users (user_id, blocked_until, reason, attack_count, is_permanent)
            VALUES (%s, %s, %s, %s, %s)
        """, (block_data.user_id, blocked_until, reason, 0, block_data.is_permanent))
        
        # Create security incident
        cur.execute("""
            INSERT INTO security_incidents 
            (user_id, incident_type, attack_indicators, sensor_data, severity, status, blocked, action_taken)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            block_data.user_id,
            "MANUAL_BLOCK",
            ["Manual block by admin"],
            json.dumps({}),
            "HIGH",
            "ACTIVE",
            True,
            f"User manually blocked by {current_user.username}: {reason}"
        ))
        
        conn.commit()
        cur.close()
        release_pg_conn(conn)
        
        # Update cache
        blocked_users_cache[block_data.user_id] = {
            "blocked_until": blocked_until,
            "is_permanent": block_data.is_permanent,
            "reason": reason,
            "attack_count": 0
        }
        
        logger.info(f"🚫 User {block_data.user_id} manually blocked by {current_user.username} ({'permanent' if block_data.is_permanent else 'temporary'})")
        return {
            "message": f"User {block_data.user_id} has been {'permanently' if block_data.is_permanent else 'temporarily'} blocked",
            "blocked_by": current_user.username,
            "is_permanent": block_data.is_permanent
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error manually blocking user: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error blocking user: {str(e)}")

@app.get("/api/security/attack-details/{record_id}")
async def get_attack_details(
    record_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(require_permission_dep(Permission.VIEW_SECURITY_LOGS))
):
    """Get detailed attack information for a specific record"""
    try:
        # Get the log record
        result = await db.execute(text("""
            SELECT * FROM cleanroom_logs WHERE id = :record_id
        """).bindparams(record_id=record_id))
        log_row = result.fetchone()
        
        if not log_row:
            raise HTTPException(status_code=404, detail="Record not found")
        
        log_data = serialize_datetime(log_row)
        
        # Get related security incident if exists
        incident_data = None
        if log_data.get("attack_status") == "CYBER_ATTACK":
            result = await db.execute(text("""
                SELECT * FROM security_incidents 
                WHERE user_id = :user_id 
                AND created_at >= :timestamp - INTERVAL '1 minute'
                ORDER BY created_at DESC
                LIMIT 1
            """).bindparams(
                user_id=log_data.get("user_id"),
                timestamp=log_data.get("timestamp")
            ))
            incident_row = result.fetchone()
            if incident_row:
                incident_data = serialize_datetime(incident_row)
        
        # Get block information if user is blocked
        block_data = None
        if log_data.get("user_id"):
            conn = get_pg_conn()
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM blocked_users 
                WHERE user_id = %s AND unblocked_at IS NULL
                ORDER BY blocked_at DESC
                LIMIT 1
            """, (log_data.get("user_id"),))
            block_row = cur.fetchone()
            cur.close()
            release_pg_conn(conn)
            
            if block_row:
                block_data = {
                    "user_id": block_row[1],
                    "blocked_at": block_row[2].isoformat() if block_row[2] else None,
                    "blocked_until": block_row[3].isoformat() if block_row[3] else None,
                    "reason": block_row[4],
                    "attack_count": block_row[5],
                    "is_permanent": block_row[6]
                }
        
        return JSONResponse({
            "log": log_data,
            "incident": incident_data,
            "block": block_data,
            "attack_frequency": user_attack_count.get(log_data.get("user_id"), 0)
        })
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching attack details: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error fetching attack details: {str(e)}")

# Helper function to get latest record from database
async def get_latest_record(room_id: Optional[int] = None):
    """Get the latest record from database for new WebSocket connections, filtered by room if specified"""
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
        if room_id is not None:
            cur.execute("""
                SELECT id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, room_id
                FROM cleanroom_logs
                WHERE room_id = %s
                ORDER BY timestamp DESC
                LIMIT 1;
            """, (room_id,))
        else:
            cur.execute("""
                SELECT id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, room_id
                FROM cleanroom_logs
                ORDER BY timestamp DESC
                LIMIT 1;
            """)
        row = cur.fetchone()
        cur.close()
        release_pg_conn(conn)
        
        if row:
            record_id, particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp, room_id = row
            
            temp = float(temperature)
            hum = float(humidity)
            press = float(pressure)
            parts = particles
            
            # Detect cyber attack using pattern analysis
            detected_attack_status, attack_indicators = detect_cyber_attack(parts, temp, hum, press, door_status, user_id)
            final_attack_status = detected_attack_status if detected_attack_status == "CYBER_ATTACK" else attack_status
            final_attack_indicators = attack_indicators if detected_attack_status == "CYBER_ATTACK" else []
            
            # Check for alerts
            temp_alert = not (TEMP_MIN <= temp <= TEMP_MAX)
            hum_alert = not (HUMIDITY_MIN <= hum <= HUMIDITY_MAX)
            press_alert = not (PRESSURE_MIN <= press <= PRESSURE_MAX)
            particle_alert = parts > PARTICLE_LIMIT
            security_alert = final_attack_status == "CYBER_ATTACK"
            has_alert = temp_alert or hum_alert or press_alert or particle_alert or security_alert
            
            return {
                "particles": parts,
                "temperature": temp,
                "humidity": hum,
                "pressure": press,
                "door_status": door_status,
                "user_id": user_id,
                "room_id": room_id,
                "attack_status": final_attack_status,
                "attack_indicators": final_attack_indicators,
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
                "alerts": {
                    "temperature": temp_alert,
                    "humidity": hum_alert,
                    "pressure": press_alert,
                    "particles": particle_alert,
                    "security": security_alert,
                    "has_alert": has_alert
                }
            }
        return None
    except Exception as e:
        logger.error(f"Error getting latest record: {e}")
        return None

# WebSocket for dashboard real time
@app.websocket("/ws/cleanroom")
async def ws_endpoint(ws: WebSocket):
    """WebSocket endpoint with optional authentication"""
    global ws_client_info  # Declare global at the top of function
    await ws.accept()
    
    # Get token from query parameters
    token = ws.query_params.get("token") if ws.query_params else None
    
    # Optional authentication check
    user = None
    if token:
        try:
            user = rbac_manager.get_user_from_token(token)
            if user and not user.has_permission(Permission.WEBSOCKET_CONNECT):
                await ws.close(code=status.WS_1008_POLICY_VIOLATION)
                logger.warning(f"WebSocket connection denied for {user.username}: Missing permission")
                return
        except Exception as e:
            logger.warning(f"WebSocket authentication error: {e}")
    
    ws_clients.append(ws)
    username = user.username if user else "anonymous"
    
    # Get user's room_id
    user_room_id = None
    if user and user.role != Role.ADMIN:
        user_room_id = await get_user_room_id(user.username)
    
    # Store client info for room filtering (use global variable)
    ws_client_info[ws] = {"user": user, "room_id": user_room_id}
    
    logger.info(f"Dashboard connected ⚡ (User: {username}, Room: {user_room_id})")
    
    # Send latest record immediately when client connects (filtered by room)
    try:
        latest = await get_latest_record(user_room_id if user and user.role != Role.ADMIN else None)
        if latest:
            await ws.send_text(json.dumps(latest))
            logger.info("📤 Sent latest record to new client")
    except Exception as e:
        logger.error(f"Error sending initial data: {e}")

    try:
        while True:
            await ws.receive_text()  # keeps alive
    except WebSocketDisconnect:
        if ws in ws_clients:
            ws_clients.remove(ws)
        ws_client_info.pop(ws, None)
        logger.info("Dashboard disconnected")

# Run the server
if __name__ == "__main__":
    import uvicorn
    logger.info("🚀 Starting Cleanroom IoT Backend Server...")
    logger.info("📊 Using PostgreSQL database (no MQTT)")
    uvicorn.run(app, host="0.0.0.0", port=8000)
