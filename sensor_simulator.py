import random
import time
import csv
import os
import json
import logging
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("sensor-sim")

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
# Remove wildcards from topic for publishing (wildcards only for subscriptions)
MQTT_TOPIC_BASE = os.getenv("MQTT_TOPIC", "cleanroom/sensors").replace("/#", "").replace("#", "")
MQTT_TOPIC = f"{MQTT_TOPIC_BASE}/Room-1/Device-1"  # Specific topic for publishing

# CSV setup (optional logging)
CSV_FILE = "cleanroom_synthetic.csv"
if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["particles","temperature","humidity","pressure","door_status","user_id","attack_status","timestamp"])
    logger.info("CSV created ✔")

# Alert ranges (for local logging only)
TEMP_MIN, TEMP_MAX = 18, 25
HUM_MIN, HUM_MAX = 40, 60
PRES_MIN, PRES_MAX = 8, 20
PARTICLE_LIMIT = 300

# MQTT Client setup
mqtt_client = mqtt.Client(client_id="cleanroom-sensor-simulator")
mqtt_connected = False

def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        logger.info("✅ Connected to MQTT broker")
    else:
        logger.error(f"❌ MQTT connection failed with code {rc}")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    logger.warning("MQTT disconnected, attempting to reconnect...")

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect

# Connect to MQTT broker
while not mqtt_connected:
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
        time.sleep(1)  # Wait for connection
        if mqtt_connected:
            break
    except Exception as e:
        logger.error(f"MQTT connection failed: {e}, retrying in 3s...")
        time.sleep(3)

logger.info("🚀 Sensor simulator ready! Publishing data every 5 seconds...")

# Generate and publish sensor data
while True:
    # Simulate occasional cyber attack (5% chance)
    is_attack = random.random() < 0.05
    attack_status = "CYBER_ATTACK" if is_attack else "NORMAL"
    
    # Generate realistic sensor data
    data = {
        "particles": random.randint(0, 500),
        "temperature": round(random.uniform(15, 30), 2),
        "humidity": round(random.uniform(30, 80), 2),
        "pressure": round(random.uniform(5, 25), 2),
        "door_status": random.choice(["OPEN", "CLOSED"]),
        "user_id": f"user_{random.randint(1, 15)}",
        "attack_status": attack_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # 🚨 Real-time alert check (local logging)
    if data["temperature"] > TEMP_MAX or data["temperature"] < TEMP_MIN:
        logger.warning(f"🚨 ALERT: Temperature {data['temperature']}°C out of range!")
    if data["humidity"] > HUM_MAX or data["humidity"] < HUM_MIN:
        logger.warning(f"🚨 ALERT: Humidity {data['humidity']}% out of range!")
    if data["pressure"] > PRES_MAX or data["pressure"] < PRES_MIN:
        logger.warning(f"🚨 ALERT: Pressure {data['pressure']}Pa out of range!")
    if data["particles"] > PARTICLE_LIMIT:
        logger.error(f"🛡 ALERT: Particle spike detected! ({data['particles']})")
    if attack_status == "CYBER_ATTACK":
        logger.error("🛡 SECURITY ALERT: Cyber attack detected!")

    # Store to CSV (optional backup)
    try:
        with open(CSV_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                data["particles"],
                data["temperature"],
                data["humidity"],
                data["pressure"],
                data["door_status"],
                data["user_id"],
                data["attack_status"],
                data["timestamp"]
            ])
    except Exception as e:
        logger.error(f"CSV write error: {e}")

    # Publish to MQTT
    if mqtt_connected:
        try:
            payload = json.dumps(data)
            result = mqtt_client.publish(MQTT_TOPIC, payload, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"📤 Published: {data['temperature']}°C, {data['humidity']}%, {data['particles']} particles")
            else:
                logger.warning(f"⚠️ MQTT publish failed: {result.rc}")
        except Exception as e:
            logger.error(f"MQTT publish error: {e}")
    else:
        logger.warning("⚠️ MQTT not connected, skipping publish")

    time.sleep(5)
