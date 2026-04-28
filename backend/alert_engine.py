import os, logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("alert-engine")

TEMP_MIN = 18
TEMP_MAX = 25
HUM_MIN  = 40
HUM_MAX  = 60
PRESS_MIN = 8
PRESS_MAX = 20
PARTICLE_LIMIT = 300

def check_alerts(data: dict):
    if not (TEMP_MIN <= data["temperature"] <= TEMP_MAX):
        logger.warning(f"🚨 ALERT: Temperature out of safe range ({data['temperature']}°C)")

    if not (HUM_MIN <= data["humidity"] <= HUM_MAX):
        logger.warning(f"🚨 ALERT: Humidity out of safe range ({data['humidity']}%)")

    if not (PRESS_MIN <= data["pressure"] <= PRESS_MAX):
        logger.warning(f"🚨 ALERT: Pressure out of safe range ({data['pressure']} Pa)")

    if data.get("attack_status") == "CYBER_ATTACK" or data["particles"] > PARTICLE_LIMIT:
        logger.error("🛡 SECURITY ALERT: Unsafe or Under Attack!")
