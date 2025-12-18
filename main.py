import os
import json
import time
import logging
import requests
import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
)
logger = logging.getLogger("ClujSentinel")

# --- CONFIGURATION FROM ENV ---
FROST_SERVER = os.getenv("FROST_SERVER", "https://frost-dev.urbreath.tech/FROST-Server/v1.1")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-api-dev.urbreath.tech")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "YOUR_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "YOUR_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "sensordata")
MINIO_PATH_PREFIX = os.getenv("MINIO_PATH_PREFIX", "cluj-napoca/air-quality/")
POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", "300"))  # Seconds (default 5 min)

# --- SENSOR MAPPING (Generated from previous analysis) ---
# Maps External Sensor ID -> { JSON Field -> FROST Datastream ID }
SENSOR_MAPPING = {
    "82000496": {"temperature": 9, "humidity": 10, "pressure": 11, "pm10": 12, "pm25": 13, "pm1": 14, "voc": 15, "noise": 16, "co2": 17, "o3": 18, "ch2o": 19},
    "8200049A": {"temperature": 20, "humidity": 21, "pressure": 22, "pm10": 23, "pm25": 24, "pm1": 25, "voc": 26, "noise": 27, "co2": 28, "o3": 29, "ch2o": 30},
    "82000495": {"temperature": 31, "humidity": 32, "pressure": 33, "pm10": 34, "pm25": 35, "pm1": 36, "voc": 37, "noise": 38, "co2": 39, "o3": 40, "ch2o": 41},
    "82000494": {"temperature": 42, "humidity": 43, "pressure": 44, "pm10": 45, "pm25": 46, "pm1": 47, "voc": 48, "noise": 49, "co2": 50, "o3": 51, "ch2o": 52},
    "82000499": {"temperature": 53, "humidity": 54, "pressure": 55, "pm10": 56, "pm25": 57, "pm1": 58, "voc": 59, "noise": 60, "co2": 61, "o3": 62, "ch2o": 63},
    "8200049B": {"temperature": 64, "humidity": 65, "pressure": 66, "pm10": 67, "pm25": 68, "pm1": 69, "voc": 70, "noise": 71, "co2": 72, "o3": 73, "ch2o": 74},
    "82000497": {"temperature": 75, "humidity": 76, "pressure": 77, "pm10": 78, "pm25": 79, "pm1": 80, "voc": 81, "noise": 82, "co2": 83, "o3": 84, "ch2o": 85},
    "82000498": {"temperature": 86, "humidity": 87, "pressure": 88, "pm10": 89, "pm25": 90, "pm1": 91, "voc": 92, "noise": 93, "co2": 94, "o3": 95, "ch2o": 96}
}

# Base URL for the external API
BASE_URL = "https://data.e-primariaclujnapoca.ro/calitate_aer/"

# Memory state to track last processed timestamp per sensor to avoid duplicates
# Format: { "82000496": "2025-12-18 10:30:29" }
last_processed_timestamps = {}

# --- MINIO CLIENT INITIALIZATION ---
try:
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4"),
    )
    logger.info(f"✅ Connected to MinIO: {MINIO_ENDPOINT}")
except Exception as e:
    logger.error(f"🚨 Error initializing MinIO client: {e}")
    s3_client = None


def upload_to_minio(sensor_id, raw_data):
    """
    Uploads the raw JSON response to MinIO.
    Path: bucket/prefix/YYYY/MM/DD/sensorID_timestamp.json
    """
    if not s3_client:
        return

    try:
        now = datetime.utcnow()
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        
        # Organize by date to avoid huge folders
        folder_structure = now.strftime("%Y/%m/%d")
        
        object_name = f"{MINIO_PATH_PREFIX}{folder_structure}/{sensor_id}_{timestamp_str}.json"
        
        # Ensure path format is clean
        object_name = object_name.replace("//", "/")

        s3_client.put_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=object_name,
            Body=json.dumps(raw_data),
            ContentType="application/json"
        )
        logger.info(f"📦 Uploaded raw data to MinIO: {object_name}")
    except Exception as e:
        logger.error(f"🚨 MinIO Upload Error: {e}")


def transform_and_send_to_frost(sensor_id, record):
    """
    Transforms a single record from the external API and sends it to FROST.
    """
    mapping = SENSOR_MAPPING.get(sensor_id)
    if not mapping:
        logger.warning(f"No mapping found for sensor {sensor_id}")
        return

    # Extract timestamp (e.g., "2025-12-18 10:30:29")
    time_str = record.get("momentul_citirii")
    if not time_str:
        return
    
    # Convert to ISO 8601 for FROST (Assuming local time is UTC or providing offset, usually safer to add 'Z' if raw is UTC)
    # Note: The raw data looks like local time. For safety, we treat string as is but format to ISO.
    try:
        # Replacing space with T and adding Z (assuming UTC for simplicity, adjust if specific timezone needed)
        iso_timestamp = time_str.replace(" ", "T") + "Z"
    except Exception:
        iso_timestamp = datetime.utcnow().isoformat() + "Z"

    # Iterate over known fields in the mapping
    for json_field, datastream_id in mapping.items():
        if json_field in record:
            raw_value = record[json_field]
            
            # Skip empty values
            if raw_value is None or raw_value == "":
                continue

            try:
                value = float(raw_value)
                
                # SPECIAL CONVERSION: Pressure from Pascal (99012) to hPa (990.12)
                if json_field == "pressure":
                    value = value / 100.0
                
                # Payload for FROST
                payload = {
                    "Datastream": {"@iot.id": datastream_id},
                    "phenomenonTime": iso_timestamp,
                    "result": value
                }

                # Send POST request
                resp = requests.post(
                    f"{FROST_SERVER}/Observations",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                
                if resp.status_code == 201:
                    logger.debug(f"✅ Obs created: Sensor {sensor_id} -> {json_field} (DS: {datastream_id})")
                else:
                    logger.error(f"❌ FROST Error {resp.status_code}: {resp.text}")

            except ValueError:
                logger.warning(f"⚠️ Could not convert value '{raw_value}' to float for {json_field}")


def process_sensor(sensor_id):
    """
    Worker function to fetch and process a single sensor.
    """
    url = f"{BASE_URL}?id_senzor={sensor_id}"
    logger.info(f"🔄 Fetching data for sensor {sensor_id}...")
    
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        # 1. Upload Raw Data to MinIO immediately
        upload_to_minio(sensor_id, data)

        # 2. Process Records for FROST
        # The API returns a list. We need to handle duplicates.
        # We sort by time to process oldest first if needed, or check newest.
        if isinstance(data, list):
            # Sort by timestamp to ensure chronological order
            data.sort(key=lambda x: x.get("momentul_citirii", ""))

            new_last_timestamp = last_processed_timestamps.get(sensor_id, "")
            
            for record in data:
                record_time = record.get("momentul_citirii")
                
                # If we have processed this sensor before, skip old records
                if sensor_id in last_processed_timestamps:
                    if record_time <= last_processed_timestamps[sensor_id]:
                        continue # Skip duplicate
                
                # Send to FROST
                transform_and_send_to_frost(sensor_id, record)
                
                # Update tracker
                if record_time > new_last_timestamp:
                    new_last_timestamp = record_time
            
            # Update the global state
            last_processed_timestamps[sensor_id] = new_last_timestamp
            
    except Exception as e:
        logger.error(f"🚨 Error processing sensor {sensor_id}: {e}")


def main_loop():
    """
    Main infinite loop that schedules the polling.
    """
    logger.info("🚀 Service Started. Listening for data (Polling mode)...")
    logger.info(f"🎯 Target FROST: {FROST_SERVER}")
    logger.info(f"⏱️ Polling Interval: {POLLING_INTERVAL} seconds")

    sensor_ids = list(SENSOR_MAPPING.keys())

    while True:
        start_time = time.time()
        
        # Use ThreadPool to fetch all sensors in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(process_sensor, sid): sid for sid in sensor_ids}
            
            for future in as_completed(futures):
                sid = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Sensor {sid} generated an exception: {exc}")

        # Calculate sleep time to maintain precise intervals
        elapsed = time.time() - start_time
        sleep_time = max(0, POLLING_INTERVAL - elapsed)
        
        logger.info(f"💤 Cycle finished in {elapsed:.2f}s. Sleeping for {sleep_time:.2f}s...")
        time.sleep(sleep_time)

if __name__ == "__main__":
    # Small startup delay to allow services to come up in Docker
    time.sleep(2)
    main_loop()