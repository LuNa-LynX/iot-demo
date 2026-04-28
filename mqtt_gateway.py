"""
MQTT Gateway for IoT Sensor Data Ingestion
Receives data from sensors via MQTT and stores in PostgreSQL
"""
import os
import json
import time
import logging
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MQTTGateway")

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "cleanroom/sensors/#")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", None)
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", None)

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+asyncpg://", "postgresql://")

# Alert Thresholds
TEMP_MIN = float(os.getenv("ALERT_TEMP_MIN", 18))
TEMP_MAX = float(os.getenv("ALERT_TEMP_MAX", 25))
HUMIDITY_MIN = float(os.getenv("ALERT_HUMIDITY_MIN", 40))
HUMIDITY_MAX = float(os.getenv("ALERT_HUMIDITY_MAX", 60))
PRESSURE_MIN = float(os.getenv("ALERT_PRESSURE_MIN", 8))
PRESSURE_MAX = float(os.getenv("ALERT_PRESSURE_MAX", 20))
PARTICLE_LIMIT = float(os.getenv("ALERT_PARTICLE_LIMIT", 300))

# PostgreSQL connection pool
pg_pool = pool.SimpleConnectionPool(1, 20, DATABASE_URL)

def get_pg_conn():
    conn = pg_pool.getconn()
    if not conn:
        raise Exception("Database connection pool exhausted")
    return conn

def release_pg_conn(conn):
    pg_pool.putconn(conn)

# MQTT Client
mqtt_client = mqtt.Client(client_id="cleanroom-mqtt-gateway", protocol=mqtt.MQTTv5)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info(f"✅ Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info(f"📡 Subscribed to topic: {MQTT_TOPIC}")
    else:
        logger.error(f"❌ MQTT connection failed with code {rc}")

def on_disconnect(client, userdata, rc, properties=None):
    logger.warning("MQTT disconnected, attempting to reconnect...")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        logger.info(f"📥 MQTT message received from topic: {msg.topic}")
        
        # Extract room/device info from topic (e.g., cleanroom/sensors/room1/ahu1)
        topic_parts = msg.topic.split('/')
        room_id = topic_parts[-2] if len(topic_parts) >= 3 else "unknown"
        device_id = topic_parts[-1] if len(topic_parts) >= 2 else "unknown"
        
        # Prepare data for storage
        sensor_data = {
            "particles": int(data.get("particles", 0)),
            "temperature": float(data.get("temperature", 0)),
            "humidity": float(data.get("humidity", 0)),
            "pressure": float(data.get("pressure", 0)),
            "door_status": data.get("door_status", "CLOSED"),
            "user_id": data.get("user_id", f"{room_id}_{device_id}"),
            "attack_status": data.get("attack_status", "NORMAL"),
            "timestamp": datetime.fromisoformat(data.get("timestamp", datetime.now(timezone.utc).isoformat())),
            "room_id": room_id,
            "device_id": device_id
        }
        
        # Check alerts
        alerts = []
        if not (TEMP_MIN <= sensor_data["temperature"] <= TEMP_MAX):
            alerts.append(f"Temperature {sensor_data['temperature']}°C out of range")
            logger.warning(f"🚨 ALERT: Temperature {sensor_data['temperature']}°C out of range [{TEMP_MIN}-{TEMP_MAX}]")
        
        if not (HUMIDITY_MIN <= sensor_data["humidity"] <= HUMIDITY_MAX):
            alerts.append(f"Humidity {sensor_data['humidity']}% out of range")
            logger.warning(f"🚨 ALERT: Humidity {sensor_data['humidity']}% out of range [{HUMIDITY_MIN}-{HUMIDITY_MAX}]")
        
        if not (PRESSURE_MIN <= sensor_data["pressure"] <= PRESSURE_MAX):
            alerts.append(f"Pressure {sensor_data['pressure']}Pa out of range")
            logger.warning(f"🚨 ALERT: Pressure {sensor_data['pressure']}Pa out of range [{PRESSURE_MIN}-{PRESSURE_MAX}]")
        
        if sensor_data["attack_status"] == "CYBER_ATTACK" or sensor_data["particles"] > PARTICLE_LIMIT:
            alerts.append("Security alert: Attack or high particles")
            logger.error(f"🛡 SECURITY ALERT: System under attack or high particles!")
        
        # Store in database
        conn = get_pg_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO cleanroom_logs 
                (particles, temperature, humidity, pressure, door_status, user_id, attack_status, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                sensor_data["particles"],
                sensor_data["temperature"],
                sensor_data["humidity"],
                sensor_data["pressure"],
                sensor_data["door_status"],
                sensor_data["user_id"],
                sensor_data["attack_status"],
                sensor_data["timestamp"]
            ))
            conn.commit()
            logger.info(f"✅ Stored data from {room_id}/{device_id}")
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
        finally:
            cur.close()
            release_pg_conn(conn)
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in MQTT message: {e}")
    except Exception as e:
        logger.error(f"Error processing MQTT message: {e}", exc_info=True)

# Assign callbacks
mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

# Authentication
if MQTT_USERNAME and MQTT_PASSWORD:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

# Connect and start
def start_mqtt_gateway():
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            logger.info("🚀 MQTT Gateway started and running...")
            break
        except Exception as e:
            logger.error(f"MQTT connection error: {e}, retrying in 3s...")
            time.sleep(3)
    
    # Keep running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down MQTT Gateway...")
        mqtt_client.loop_stop()
        mqtt_client.disconnect()

if __name__ == "__main__":
    start_mqtt_gateway()

