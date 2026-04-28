import os
from dotenv import load_dotenv

load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT   = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC  = os.getenv("MQTT_TOPIC")

ALERT_TEMP_MIN = float(os.getenv("ALERT_TEMP_MIN"))
ALERT_TEMP_MAX = float(os.getenv("ALERT_TEMP_MAX"))
ALERT_HUM_MIN  = float(os.getenv("ALERT_HUMIDITY_MIN"))
ALERT_HUM_MAX  = float(os.getenv("ALERT_HUMIDITY_MAX"))
ALERT_PRESS_MIN = float(os.getenv("ALERT_PRESSURE_MIN"))
ALERT_PRESS_MAX = float(os.getenv("ALERT_PRESSURE_MAX"))
