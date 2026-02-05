import json
import logging
import os
import requests
import azure.functions as func
#from azure.iot.device import IoTHubDeviceClient, Message

#setup
WEATHER_URL = "https://weather.googleapis.com/v1/currentConditions:lookup"

LOCATIONS = [
    {"location_id": "bangkok", "latitude": 13.7563, "longitude": 100.5018},
    {"location_id": "ayutthaya", "latitude": 14.3550, "longitude": 100.5650}
]

def get_current_weather(api_key: str, latitude: float, longitude: float, timeout: int = 10):
    params = {
        "key": api_key,
        "location.latitude": latitude,
        "location.longitude": longitude
    }

    response = requests.get(WEATHER_URL, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()

    return {
        "timestamp": data.get("currentTime"),
        "temperature_c": data.get("temperature", {}).get("degrees"),
        "humidity_percent": data.get("relativeHumidity"),
        "uv_index": data.get("uvIndex")
    }

def send_telemetry(connection_string: str, payload: dict):
    client = IoTHubDeviceClient.create_from_connection_string(connection_string)
    try:
        client.connect()
        msg = Message(json.dumps(payload))
        msg.content_encoding = "utf-8"
        msg.content_type = "application/json"
        client.send_message(msg)
    finally:
        client.disconnect()

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Weather → IoT Hub function started")

    # ✅ ดึง ENV ที่นี่เท่านั้น
    google_api_key = os.getenv("GOOGLE_API_KEY")
    conn_bkk = os.getenv("IOT_CONN_BANGKOK")
    conn_ayt = os.getenv("IOT_CONN_AYUTTHAYA")

    if not google_api_key or not conn_bkk or not conn_ayt:
        logging.error("Missing environment variables")
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
            logging.error(f"Weather error {location_id}: {e}")
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
        send_telemetry(device_connections[location_id], telemetry)

    logging.info("Weather → IoT Hub function finished")
