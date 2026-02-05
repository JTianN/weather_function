import json
import logging
import os
import requests
import azure.functions as func
from azure.iot.device import IoTHubDeviceClient, Message

# =====================================================
# CONFIG
# =====================================================
WEATHER_URL = "https://weather.googleapis.com/v1/currentConditions:lookup"

LOCATIONS = [
    {"location_id": "bangkok", "latitude": 13.7563, "longitude": 100.5018},
    {"location_id": "ayutthaya", "latitude": 14.3550, "longitude": 100.5650}
]

# cache IoT clients (สำคัญมากบน Azure)
IOT_CLIENTS = {}

# =====================================================
# GOOGLE WEATHER
# =====================================================
def get_current_weather(api_key: str, latitude: float, longitude: float, timeout: int = 10):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json"
    }

    params = {
        "key": api_key,
        "location.latitude": latitude,
        "location.longitude": longitude
    }

    response = requests.get(
        WEATHER_URL,
        params=params,
        headers=headers,
        timeout=timeout
    )

    logging.info(f"Weather status={response.status_code}")
    logging.debug(f"Weather raw={response.text}")

    response.raise_for_status()
    data = response.json()

    return {
        "timestamp": data.get("currentTime"),
        "temperature_c": data.get("temperature", {}).get("degrees"),
        "humidity_percent": data.get("relativeHumidity"),
        "uv_index": data.get("uvIndex")
    }

# =====================================================
# IOT HUB CLIENT (REUSE)
# =====================================================
def get_iot_client(connection_string: str) -> IoTHubDeviceClient:
    if connection_string not in IOT_CLIENTS:
        client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        client.connect()
        IOT_CLIENTS[connection_string] = client
        logging.info("IoT Hub connected (new client)")
    return IOT_CLIENTS[connection_string]

def send_telemetry(connection_string: str, payload: dict):
    client = get_iot_client(connection_string)

    message = Message(json.dumps(payload))
    message.content_encoding = "utf-8"
    message.content_type = "application/json"

    client.send_message(message)

# =====================================================
# AZURE FUNCTION ENTRY
# =====================================================
def main(mytimer: func.TimerRequest) -> None:
    logging.warning("=== WEATHER FUNCTION STARTED ===")

    google_api_key = os.getenv("GOOGLE_API_KEY")
    conn_bkk = os.getenv("IOT_CONN_BANGKOK")
    conn_ayt = os.getenv("IOT_CONN_AYUTTHAYA")

    if not google_api_key or not conn_bkk or not conn_ayt:
        logging.error("Missing ENV variables")
        return

    device_connections = {
        "bangkok": conn_bkk,
        "ayutthaya": conn_ayt
    }

    for loc in LOCATIONS:
        location_id = loc["location_id"]

        try:
            weather = get_current_weather(
                google_api_key,
                loc["latitude"],
                loc["longitude"]
            )
        except Exception as e:
            logging.error(f"Weather error ({location_id}): {e}")
            continue

        telemetry = {
            "schemaVersion": "v1",
            "source": "google-weather",
            "locationId": location_id,
            "timestamp": weather["timestamp"],
            "temp": weather["temperature_c"],
            "humi": weather["humidity_percent"],
            "uv_index": weather["uv_index"]
        }

        logging.info(f"Sending telemetry → {location_id}")
        logging.info(telemetry)

        send_telemetry(
            device_connections[location_id],
            telemetry
        )

    logging.warning("=== WEATHER FUNCTION FINISHED ===")
