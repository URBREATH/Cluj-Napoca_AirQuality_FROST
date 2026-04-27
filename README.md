# Cluj-Napoca Air Quality — FROST Ingestion Service
Provided by: ENG

## Description
Automated service that bridges the **Cluj-Napoca open air quality API** with a **FROST Server** (OGC SensorThings API v1.1) and a **MinIO** object store.

The service continuously polls the public sensor API at a configurable interval. For each sensor, it archives the raw JSON response to MinIO and forwards only **new observations** to FROST — deduplication is handled automatically by persisting the last processed timestamp to disk, so no duplicate data is created even after a container restart.

Timezone handling is exact: the source API returns Romanian local time (`Europe/Bucharest`, UTC+2/+3 depending on DST), which is converted to a proper ISO 8601 offset timestamp before being sent to FROST.

## Monitored sensors and measured quantities

Each sensor reports the following quantities. The mapping between sensor IDs and FROST Datastream IDs is defined in [`sensor_mapping.json`](sensor_mapping.json) and can be updated without modifying the source code.

| Field | Physical quantity | Unit | Notes |
|-------|-------------------|------|-------|
| `temperature` | Air temperature | °C | |
| `humidity` | Relative humidity | % | |
| `pressure` | Atmospheric pressure | hPa | Source value is in Pa — divided by 100 automatically |
| `pm10` | Particulate matter ≤ 10 µm | µg/m³ | |
| `pm25` | Particulate matter ≤ 2.5 µm | µg/m³ | |
| `pm1` | Particulate matter ≤ 1 µm | µg/m³ | |
| `voc` | Volatile organic compounds | index | |
| `noise` | Ambient noise | dB | |
| `co2` | Carbon dioxide | ppm | |
| `o3` | Ozone | µg/m³ | |
| `ch2o` | Formaldehyde | µg/m³ | |

## How it works

1. At startup, the service loads the last known timestamp for each sensor from `STATE_FILE` (persisted across restarts).
2. Every `POLLING_INTERVAL` seconds, all sensors are fetched in parallel (up to `POLLING_WORKERS` concurrent threads).
3. For each sensor response:
   - The raw JSON is uploaded to MinIO under `PREFIX/YYYY/MM/DD/SENSOR_ID_HHMMSS.json`.
   - Only records **newer** than the last processed timestamp are forwarded to FROST.
   - The new high-water mark is saved immediately to disk.
4. A heartbeat file is updated at the end of each cycle so Docker can detect a hung service.

## Sensor mapping

Sensor-to-datastream mapping is loaded at startup from [`sensor_mapping.json`](sensor_mapping.json). The file is a JSON object keyed by the external sensor ID; each value maps field names to FROST Datastream `@iot.id` values.

```json
{
  "82000496": {
    "temperature": 9,
    "humidity": 10,
    "pressure": 11,
    "pm10": 12,
    "pm25": 13,
    "pm1": 14,
    "voc": 15,
    "noise": 16,
    "co2": 17,
    "o3": 18,
    "ch2o": 19
  }
}
```

To add a new sensor, append its entry to `sensor_mapping.json` and restart the service. If the file is absent, the service falls back to the built-in mapping defined in `main.py`.

## State management

Processed timestamps are stored in `STATE_FILE` (default `/app/state/last_processed.json`), which is mounted as a host volume so it survives container restarts:

```json
{
  "82000496": "2025-12-18 10:30:29",
  "8200049A": "2025-12-18 10:28:17"
}
```

To force a full re-ingestion from scratch, delete or empty this file before restarting the container. To avoid re-ingesting historical data on first deploy, pre-populate the file with the timestamp of the last observation already present in FROST for each sensor.

## MinIO folder layout

Raw data is stored under `MINIO_PATH_PREFIX` with a date-based hierarchy to keep folders manageable:

```
sensordata/
└── cluj-napoca/air-quality/
    └── 2025/
        └── 12/
            └── 18/
                ├── 82000496_20251218_103029.json
                └── 8200049A_20251218_102817.json
```

## Deployment

The service is designed to run as a long-lived container. A `./state/` directory is mounted at `/app/state` inside the container to persist both the deduplication state and the healthcheck heartbeat.

```bash
docker compose up -d
```

Docker will automatically restart the container on failure (`restart: always`) and report it as unhealthy if no polling cycle completes within 10 minutes.

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FROST_SERVER` | Full base URL of the FROST Server (v1.1). | `https://frost-dev.urbreath.tech/FROST-Server/v1.1` |
| `FROST_TIMEOUT` | HTTP timeout for every FROST request (seconds). | `15` |
| `MINIO_ENDPOINT` | MinIO hostname (without protocol). | `minio-api-dev.urbreath.tech` |
| `MINIO_ACCESS_KEY` | MinIO access key. Leave empty to disable uploads. | _(empty)_ |
| `MINIO_SECRET_KEY` | MinIO secret key. | _(empty)_ |
| `MINIO_BUCKET_NAME` | Target bucket name. | `sensordata` |
| `MINIO_PATH_PREFIX` | Key prefix inside the bucket. | `cluj-napoca/air-quality/` |
| `SOURCE_API_URL` | Base URL of the Cluj-Napoca open data API. | `https://data.e-primariaclujnapoca.ro/calitate_aer/` |
| `POLLING_INTERVAL` | Seconds between polling cycles. | `300` |
| `POLLING_WORKERS` | Number of sensors fetched concurrently. | `8` |
| `STATE_FILE` | Path to the deduplication state file (inside container). | `/app/state/last_processed.json` |
| `HEARTBEAT_FILE` | Path to the healthcheck heartbeat file. | `/app/state/heartbeat` |
| `MAPPING_FILE` | Path to the sensor mapping JSON file. | `/app/sensor_mapping.json` |

Copy [`.env.example`](.env.example) to `.env` and fill in the credentials before starting the service.

## External references

- [OGC SensorThings API — FROST Server](https://fraunhoferiosb.github.io/FROST-Server/)
- [Cluj-Napoca Open Data Portal](https://data.e-primariaclujnapoca.ro)
- [MinIO Python SDK (boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
