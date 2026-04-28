import os
import json
import time
import logging
import threading
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
from psycopg2 import pool
from dotenv import load_dotenv

# -------------------- ENV --------------------
load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC  = os.getenv("MQTT_TOPIC")

DB_URL = os.getenv("DATABASE_URL").replace(
    "postgresql+asyncpg://", "postgresql://"
)

# -------------------- LOGGER --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("mqtt-client")

# -------------------- DB POOL --------------------
pg_pool = pool.SimpleConnectionPool(2, 20, DB_URL)

def get_pg():
    conn = pg_pool.getconn()
    if not conn:
        raise RuntimeError("PostgreSQL pool exhausted")
    return conn

def release_pg(conn):
    pg_pool.putconn(conn)

# -------------------- DB INIT --------------------
def init_db():
    conn = get_pg()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cleanroom_logs (
            id SERIAL PRIMARY KEY,
            particles INT NOT NULL,
            temperature REAL NOT NULL,
            humidity REAL NOT NULL,
            pressure REAL NOT NULL,
            door_status TEXT NOT NULL,
            user_id TEXT NOT NULL,
            attack_status TEXT NOT NULL DEFAULT 'NORMAL',
            timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)
    conn.commit()
    cur.close()
    release_pg(conn)
    logger.info("PostgreSQL table ready ✔")

# -------------------- MQTT CALLBACKS --------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("MQTT connected ✔")
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info(f"Subscribed → {MQTT_TOPIC}")
    else:
        logger.error(f"MQTT connection failed: {reason_code}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        logger.info("📥 MQTT data received")

        ts = data.get("timestamp")
        if ts:
            try:
                if ts.endswith("Z"):
                    ts = ts.replace("Z", "+00:00")
                dt = datetime.fromisoformat(ts)
            except:
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)

        conn = get_pg()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO cleanroom_logs
            (particles, temperature, humidity, pressure,
             door_status, user_id, attack_status, timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            data["particles"],
            data["temperature"],
            data["humidity"],
            data["pressure"],
            data["door_status"],
            data["user_id"],
            data.get("attack_status", "NORMAL"),
            dt
        ))
        conn.commit()
        cur.close()
        release_pg(conn)

        logger.info("DB insert ✔")

        if data.get("attack_status") == "CYBER_ATTACK":
            logger.warning("🚨 CYBER ATTACK EVENT")

    except Exception as e:
        logger.error(f"MQTT processing error: {e}")

# -------------------- MQTT THREAD --------------------
def start_mqtt():
    def _run():
        client = mqtt.Client(
            client_id="cleanroom-backend",
            protocol=mqtt.MQTTv5
        )
        client.on_connect = on_connect
        client.on_message = on_message

        while True:
            try:
                client.connect(MQTT_BROKER, MQTT_PORT, 60)
                client.loop_forever()
            except Exception as e:
                logger.error(f"MQTT error: {e}, retrying...")
                time.sleep(3)

    threading.Thread(target=_run, daemon=True).start()
    logger.info("MQTT background listener started ⚡")

# -------------------- INIT CALL --------------------
init_db()
