import json
import logging
import os
import requests
import azure.functions as func
from azure.iot.device import IoTHubDeviceClient, Message

# =====================================================
# CONFIG (ENV)
# =====================================================
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
WEATHER_URL = "https://weather.googleapis.com/v1/currentConditions:lookup"

# Multi locations
LOCATIONS = [
    {
        "location_id": "bangkok",
        "latitude": 13.7563,
        "longitude": 100.5018
    },
    {
        "location_id": "ayutthaya",
        "latitude": 14.3550,
        "longitude": 100.5650
    }
]

# IoT Hub device connection strings (ENV)
DEVICE_CONNECTIONS = {
    "bangkok": os.environ["IOT_CONN_BANGKOK"],
    "ayutthaya": os.environ["IOT_CONN_AYUTTHAYA"]
}

# =====================================================
# GOOGLE WEATHER (ของเดิมคุณ ใช้ได้ตรง ๆ)
# =====================================================
def get_current_weather(latitude: float, longitude: float, timeout: int = 10) -> dict | None:
    params = {
        "key": GOOGLE_API_KEY,
        "location.latitude": latitude,
        "location.longitude": longitude
    }

    try:
        response = requests.get(
            WEATHER_URL,
            params=params,
            timeout=timeout
        )
        response.raise_for_status()
        data = response.json()

        return {
            "timestamp": data.get("currentTime"),
            "temperature_c": data.get("temperature", {}).get("degrees"),
            "humidity_percent": data.get("relativeHumidity"),
            "uv_index": data.get("uvIndex")
        }

    except Exception as e:
        logging.error(f"Weather API error: {e}")
        return None

# =====================================================
# SEND TELEMETRY
# =====================================================
def send_telemetry(connection_string: str, payload: dict):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    try:
        client.connect()

        message = Message(json.dumps(payload))
        message.content_encoding = "utf-8"
        message.content_type = "application/json"

        client.send_message(message)

    finally:
        client.disconnect()

# =====================================================
# AZURE FUNCTION ENTRY POINT
# =====================================================
def main(mytimer: func.TimerRequest) -> None:
    logging.info("Weather → IoT Hub function started")

    for loc in LOCATIONS:
        location_id = loc["location_id"]

        weather = get_current_weather(
            loc["latitude"],
            loc["longitude"]
        )

        if not weather:
            logging.warning(f"Skip {location_id} (weather unavailable)")
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

        logging.info(f"Sending telemetry → {location_id}: {telemetry}")

        send_telemetry(
            DEVICE_CONNECTIONS[location_id],
            telemetry
        )

    logging.info("Weather → IoT Hub function finished")
