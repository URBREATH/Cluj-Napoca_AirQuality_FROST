import os
import json
import time
import signal
import logging
import threading
import requests
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
)
logger = logging.getLogger("ClujSentinel")

# --- CONFIGURATION FROM ENV ---
FROST_SERVER     = os.getenv("FROST_SERVER",     "https://frost-dev.urbreath.tech/FROST-Server/v1.1")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio-api-dev.urbreath.tech")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET_NAME  = os.getenv("MINIO_BUCKET_NAME",  "sensordata")
MINIO_PATH_PREFIX  = os.getenv("MINIO_PATH_PREFIX",  "cluj-napoca/air-quality/").strip("/")
POLLING_INTERVAL   = int(os.getenv("POLLING_INTERVAL", "300"))
POLLING_WORKERS    = int(os.getenv("POLLING_WORKERS",  "8"))
FROST_TIMEOUT      = int(os.getenv("FROST_TIMEOUT",    "15"))
SOURCE_API_URL     = os.getenv("SOURCE_API_URL",  "https://data.e-primariaclujnapoca.ro/calitate_aer/")
STATE_FILE         = Path(os.getenv("STATE_FILE",      "/app/state/last_processed.json"))
HEARTBEAT_FILE     = Path(os.getenv("HEARTBEAT_FILE",  "/app/state/heartbeat"))
MAPPING_FILE       = Path(os.getenv("MAPPING_FILE",    "/app/sensor_mapping.json"))

BUCHAREST_TZ = ZoneInfo("Europe/Bucharest")

# --- SENSOR MAPPING ---
# Loaded from MAPPING_FILE at startup; falls back to the inline dict if the file is missing.
_BUILTIN_MAPPING = {
    "82000496": {"temperature": 9,  "humidity": 10, "pressure": 11, "pm10": 12, "pm25": 13, "pm1": 14, "voc": 15, "noise": 16, "co2": 17, "o3": 18, "ch2o": 19},
    "8200049A": {"temperature": 20, "humidity": 21, "pressure": 22, "pm10": 23, "pm25": 24, "pm1": 25, "voc": 26, "noise": 27, "co2": 28, "o3": 29, "ch2o": 30},
    "82000495": {"temperature": 31, "humidity": 32, "pressure": 33, "pm10": 34, "pm25": 35, "pm1": 36, "voc": 37, "noise": 38, "co2": 39, "o3": 40, "ch2o": 41},
    "82000494": {"temperature": 42, "humidity": 43, "pressure": 44, "pm10": 45, "pm25": 46, "pm1": 47, "voc": 48, "noise": 49, "co2": 50, "o3": 51, "ch2o": 52},
    "82000499": {"temperature": 53, "humidity": 54, "pressure": 55, "pm10": 56, "pm25": 57, "pm1": 58, "voc": 59, "noise": 60, "co2": 61, "o3": 62, "ch2o": 63},
    "8200049B": {"temperature": 64, "humidity": 65, "pressure": 66, "pm10": 67, "pm25": 68, "pm1": 69, "voc": 70, "noise": 71, "co2": 72, "o3": 73, "ch2o": 74},
    "82000497": {"temperature": 75, "humidity": 76, "pressure": 77, "pm10": 78, "pm25": 79, "pm1": 80, "voc": 81, "noise": 82, "co2": 83, "o3": 84, "ch2o": 85},
    "82000498": {"temperature": 86, "humidity": 87, "pressure": 88, "pm10": 89, "pm25": 90, "pm1": 91, "voc": 92, "noise": 93, "co2": 94, "o3": 95, "ch2o": 96},
}

def _load_sensor_mapping():
    if MAPPING_FILE.exists():
        try:
            with open(MAPPING_FILE) as f:
                mapping = json.load(f)
            logger.info(f"Loaded sensor mapping from {MAPPING_FILE} ({len(mapping)} sensors)")
            return mapping
        except Exception as e:
            logger.warning(f"Could not load {MAPPING_FILE}: {e} — using built-in mapping")
    return _BUILTIN_MAPPING

SENSOR_MAPPING = _load_sensor_mapping()

# --- STATE PERSISTENCE ---
_state_lock = threading.Lock()

def _load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            logger.warning("Could not read state file, starting fresh")
    return {}

def _save_state(state: dict):
    with _state_lock:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        tmp.replace(STATE_FILE)

last_processed_timestamps: dict = _load_state()

# --- GRACEFUL SHUTDOWN ---
_shutdown = threading.Event()

def _on_shutdown(signum, frame):
    logger.info("Shutdown signal received, stopping after current cycle...")
    _shutdown.set()

signal.signal(signal.SIGTERM, _on_shutdown)
signal.signal(signal.SIGINT, _on_shutdown)

# --- MINIO CLIENT ---
def _make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=boto3.session.Config(signature_version="s3v4"),
    )

s3_client = None
if MINIO_ACCESS_KEY and MINIO_SECRET_KEY:
    try:
        s3_client = _make_s3_client()
        logger.info(f"Connected to MinIO: {MINIO_ENDPOINT}")
    except Exception as e:
        logger.error(f"Error initializing MinIO client: {e}")
else:
    logger.warning("MinIO credentials not set — uploads disabled")

# --- FROST SESSION (connection reuse) ---
frost_session = requests.Session()
frost_session.headers.update({"Content-Type": "application/json"})


def upload_to_minio(sensor_id: str, raw_data):
    global s3_client
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        return

    if s3_client is None:
        try:
            s3_client = _make_s3_client()
        except Exception as e:
            logger.error(f"MinIO reconnect failed: {e}")
            return

    try:
        now = datetime.now(timezone.utc)
        folder = now.strftime("%Y/%m/%d")
        timestamp_str = now.strftime("%Y%m%d_%H%M%S")
        object_name = f"{MINIO_PATH_PREFIX}/{folder}/{sensor_id}_{timestamp_str}.json"

        s3_client.put_object(
            Bucket=MINIO_BUCKET_NAME,
            Key=object_name,
            Body=json.dumps(raw_data),
            ContentType="application/json",
        )
        logger.info(f"Uploaded raw data to MinIO: {object_name}")
    except Exception as e:
        logger.error(f"MinIO upload error: {e}")
        s3_client = None  # force reconnect on next attempt


def transform_and_send_to_frost(sensor_id: str, record: dict):
    mapping = SENSOR_MAPPING.get(sensor_id)
    if not mapping:
        logger.warning(f"No mapping found for sensor {sensor_id}")
        return

    time_str = record.get("momentul_citirii")
    if not time_str:
        return

    try:
        # API returns local Romanian time — attach correct timezone before converting
        local_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BUCHAREST_TZ)
        iso_timestamp = local_dt.isoformat()
    except Exception:
        iso_timestamp = datetime.now(timezone.utc).isoformat()

    for json_field, datastream_id in mapping.items():
        if json_field not in record:
            continue
        raw_value = record[json_field]
        if raw_value is None or raw_value == "":
            continue

        try:
            value = float(raw_value)
            if json_field == "pressure":
                value /= 100.0  # Pa → hPa

            payload = {
                "Datastream": {"@iot.id": datastream_id},
                "phenomenonTime": iso_timestamp,
                "result": value,
            }

            resp = frost_session.post(
                f"{FROST_SERVER}/Observations",
                json=payload,
                timeout=FROST_TIMEOUT,
            )

            if resp.status_code == 201:
                logger.debug(f"Obs created: sensor {sensor_id} / {json_field} (DS {datastream_id})")
            else:
                logger.error(f"FROST error {resp.status_code} for sensor {sensor_id}/{json_field}: {resp.text}")

        except ValueError:
            logger.warning(f"Cannot convert '{raw_value}' to float for {json_field} (sensor {sensor_id})")
        except requests.Timeout:
            logger.error(f"FROST timeout for sensor {sensor_id} / {json_field}")


def process_sensor(sensor_id: str):
    url = f"{SOURCE_API_URL}?id_senzor={sensor_id}"
    logger.info(f"Fetching data for sensor {sensor_id}...")

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

        upload_to_minio(sensor_id, data)

        if not isinstance(data, list):
            return

        data.sort(key=lambda x: x.get("momentul_citirii", ""))

        last_ts = last_processed_timestamps.get(sensor_id, "")
        new_last_ts = last_ts

        for record in data:
            record_time = record.get("momentul_citirii", "")
            if record_time <= last_ts:
                continue
            transform_and_send_to_frost(sensor_id, record)
            if record_time > new_last_ts:
                new_last_ts = record_time

        if new_last_ts != last_ts:
            last_processed_timestamps[sensor_id] = new_last_ts
            _save_state(last_processed_timestamps)

    except Exception as e:
        logger.error(f"Error processing sensor {sensor_id}: {e}")


def main_loop():
    logger.info("Service started (polling mode)")
    logger.info(f"Target FROST: {FROST_SERVER}")
    logger.info(f"Polling interval: {POLLING_INTERVAL}s  Workers: {POLLING_WORKERS}")

    sensor_ids = list(SENSOR_MAPPING.keys())
    HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)

    while not _shutdown.is_set():
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=POLLING_WORKERS) as executor:
            futures = {executor.submit(process_sensor, sid): sid for sid in sensor_ids}
            for future in as_completed(futures):
                sid = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error(f"Sensor {sid} raised an exception: {exc}")

        HEARTBEAT_FILE.touch()

        elapsed = time.time() - start_time
        sleep_time = max(0, POLLING_INTERVAL - elapsed)
        logger.info(f"Cycle done in {elapsed:.2f}s. Next run in {sleep_time:.2f}s.")
        _shutdown.wait(timeout=sleep_time)

    logger.info("Service stopped.")


if __name__ == "__main__":
    time.sleep(2)
    main_loop()
