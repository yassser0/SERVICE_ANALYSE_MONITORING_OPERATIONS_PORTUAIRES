import os
import time
import json
import uuid
import random
import math
from datetime import datetime, timezone
import logging
from confluent_kafka import Producer

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC             = os.getenv("KAFKA_TOPIC", "port-iot-data")
EMIT_INTERVAL_SECONDS   = int(os.getenv("EMIT_INTERVAL_SECONDS", "3"))
NUM_SENSORS             = int(os.getenv("NUM_SENSORS", "20"))
LOG_LEVEL               = os.getenv("LOG_LEVEL", "INFO")
ANOMALY_PROBABILITY     = float(os.getenv("ANOMALY_PROBABILITY", "0.05"))

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Simulator")

# ─── Port Zones ───────────────────────────────────────────────────────────────
ZONES = ['QUAY_A', 'QUAY_B', 'QUAY_C', 'YARD_NORTH', 'YARD_SOUTH',
         'GATE_ENTRY', 'GATE_EXIT', 'WAREHOUSE_1', 'WAREHOUSE_2', 'MAINTENANCE_AREA']

# Zones where speed limits are enforced (tight areas)
SPEED_LIMITED_ZONES = {'YARD_NORTH', 'YARD_SOUTH', 'WAREHOUSE_1', 'WAREHOUSE_2', 'GATE_ENTRY', 'GATE_EXIT'}

EQUIPMENT_STATUSES = ['ACTIVE', 'ACTIVE', 'ACTIVE', 'ACTIVE', 'IDLE', 'MAINTENANCE']

# ─── Vessels (mirrors DB seed data) ──────────────────────────────────────────
VESSELS = [
    {"vessel_id": "MSC-AURORA",  "zone": "QUAY_A"},
    {"vessel_id": "CMA-LIBERTY", "zone": "QUAY_B"},
    {"vessel_id": "EVER-BRIGHT", "zone": "QUAY_C"},
    {"vessel_id": "MAERSK-STAR", "zone": "YARD_NORTH"},
    {"vessel_id": "COS-PACIFIC", "zone": "YARD_SOUTH"},
]

# ─── Containers (persistent state per container) ──────────────────────────────
CONTAINER_IDS = [f"CMAU{random.randint(1000000, 9999999)}" for _ in range(50)]

# Each container has a persistent GPS position that drifts slightly each tick
_container_state: dict = {}

def _init_container_state(container_id: str) -> dict:
    vessel = random.choice(VESSELS)
    lat = round(random.uniform(33.55, 33.95), 6)
    lon = round(random.uniform(-7.95, -7.55), 6)
    return {
        "lat": lat,
        "lon": lon,
        "vessel_id": vessel["vessel_id"],
        "zone": vessel["zone"],
        "heading": random.uniform(0, 360),
    }

for cid in CONTAINER_IDS:
    _container_state[cid] = _init_container_state(cid)

# ─── Sensors ──────────────────────────────────────────────────────────────────
SENSORS = (
    [{"sensor_id": f"SNS-GPS-{i:03d}",  "type": "GPS"}              for i in range(1, 5)] +
    [{"sensor_id": f"SNS-TEMP-{i:03d}", "type": "TEMPERATURE"}       for i in range(1, 5)] +
    [{"sensor_id": f"SNS-MOV-{i:03d}",  "type": "MOVEMENT"}          for i in range(1, 5)] +
    [{"sensor_id": f"SNS-EQP-{i:03d}",  "type": "EQUIPMENT_STATUS"}  for i in range(1, 5)] +
    [{"sensor_id": f"SNS-CMB-{i:03d}",  "type": "COMBINED"}          for i in range(1, 5)]
)

# Persistent sensor state (battery drains over time)
_sensor_state: dict = {}
for s in SENSORS:
    _sensor_state[s["sensor_id"]] = {
        "battery": random.uniform(60.0, 100.0),   # starting battery %
        "signal":  random.uniform(-75.0, -45.0),  # dBm
        "operating_hours": random.uniform(100, 5000),
    }


# ─── Helpers ──────────────────────────────────────────────────────────────────

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def _drift_gps(lat: float, lon: float, heading: float) -> tuple[float, float, float]:
    """Move GPS position by a tiny realistic step (0–3 metres)."""
    step = random.uniform(0.0, 0.00003)   # ~0–3 m per tick
    heading += random.uniform(-5, 5)      # slight heading change
    heading %= 360
    dlat = step * math.cos(math.radians(heading))
    dlon = step * math.sin(math.radians(heading))
    return round(lat + dlat, 6), round(lon + dlon, 6), heading


def generate_sensor_data(sensor: dict) -> dict:
    sid   = sensor["sensor_id"]
    stype = sensor["type"]
    state = _sensor_state[sid]

    # ── Battery drain (0.05–0.2 % per tick) ──────────────────────────────────
    state["battery"] = max(0.0, state["battery"] - random.uniform(0.05, 0.2))
    state["operating_hours"] += EMIT_INTERVAL_SECONDS / 3600.0
    # Signal jitter
    state["signal"] = round(state["signal"] + random.uniform(-2.0, 2.0), 1)
    state["signal"] = max(-100.0, min(-30.0, state["signal"]))

    # Pick a container
    container_id = random.choice(CONTAINER_IDS) if random.random() > 0.2 else None
    cstate = _container_state.get(container_id) if container_id else None

    is_anomaly = random.random() < ANOMALY_PROBABILITY

    payload: dict = {
        "event_id":        str(uuid.uuid4()),
        "sensor_id":       sid,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "zone":            cstate["zone"] if cstate else random.choice(ZONES),
        "container_id":    container_id,
        "vessel_id":       cstate["vessel_id"] if cstate else None,
        "battery_level":   round(state["battery"], 2),
        "signal_strength": state["signal"],
    }

    # ── GPS / MOVEMENT sensors ────────────────────────────────────────────────
    if stype in ("GPS", "MOVEMENT", "COMBINED"):
        if cstate:
            # Smooth drift
            new_lat, new_lon, new_hdg = _drift_gps(
                cstate["lat"], cstate["lon"], cstate["heading"])
            cstate["lat"], cstate["lon"], cstate["heading"] = new_lat, new_lon, new_hdg
            lat, lon = new_lat, new_lon
        else:
            lat = round(random.uniform(33.55, 33.95), 6)
            lon = round(random.uniform(-7.95, -7.55), 6)

        if is_anomaly and random.random() < 0.3:
            # Invalid GPS outside port bounds
            payload["latitude"]  = round(random.uniform(35.0, 37.0), 6)
            payload["longitude"] = round(random.uniform(-5.0, -3.0), 6)
        else:
            payload["latitude"]  = lat
            payload["longitude"] = lon

        zone = payload["zone"]
        # Speed: port vehicles move slowly; anomaly = speed violation
        if is_anomaly and zone in SPEED_LIMITED_ZONES and random.random() < 0.4:
            payload["speed_kmh"] = round(random.uniform(26.0, 45.0), 1)
        else:
            payload["speed_kmh"] = round(random.uniform(0.5, 18.0), 1)

        payload["heading_degrees"] = round(random.uniform(0, 360), 1)

    # ── TEMPERATURE sensors ───────────────────────────────────────────────────
    if stype in ("TEMPERATURE", "COMBINED"):
        if is_anomaly and random.random() < 0.5:
            payload["temperature"] = round(random.uniform(86.0, 105.0), 2)
        else:
            # Realistic drift: cold-chain containers kept 0–8 °C; others 18–30 °C
            if "TEMP" in sid:
                payload["temperature"] = round(random.uniform(2.0, 8.5), 2)
            else:
                payload["temperature"] = round(random.uniform(18.0, 32.0), 2)
        payload["humidity"] = round(random.uniform(40.0, 95.0), 1)

    # ── EQUIPMENT STATUS sensors ──────────────────────────────────────────────
    if stype in ("EQUIPMENT_STATUS", "COMBINED"):
        if is_anomaly and stype == "EQUIPMENT_STATUS" and random.random() < 0.6:
            payload["equipment_status"] = "ERROR"
        else:
            payload["equipment_status"] = random.choice(EQUIPMENT_STATUSES)
        payload["load_percentage"]  = round(random.uniform(0.0, 100.0), 1)
        payload["operating_hours"]  = round(state["operating_hours"], 1)

    return payload


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info(f"Starting IoT Simulator → Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    logger.info(f"Anomaly probability: {ANOMALY_PROBABILITY*100:.1f}%  |  Sensors: {len(SENSORS)}  |  Containers: {len(CONTAINER_IDS)}")

    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 'client.id': 'iot-simulator'}

    producer = None
    retries = 15
    while retries > 0:
        try:
            producer = Producer(conf)
            # Quick connectivity test
            producer.list_topics(timeout=5)
            logger.info("Connected to Kafka successfully.")
            break
        except Exception as e:
            logger.warning(f"Kafka not ready ({e}). Retrying in 5 s... ({retries} left)")
            retries -= 1
            time.sleep(5)

    if not producer:
        logger.error("Could not connect to Kafka after retries. Exiting.")
        return

    tick = 0
    try:
        while True:
            tick += 1
            num_emit = random.randint(3, min(NUM_SENSORS, len(SENSORS)))
            sensors_to_emit = random.sample(SENSORS, num_emit)

            for sensor in sensors_to_emit:
                payload = generate_sensor_data(sensor)
                producer.produce(
                    KAFKA_TOPIC,
                    key=payload["sensor_id"],
                    value=json.dumps(payload),
                    callback=delivery_report,
                )

            producer.poll(0)
            logger.info(f"[Tick {tick}] Emitted {num_emit} sensor events.")
            time.sleep(EMIT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Simulator stopped by user.")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()


if __name__ == "__main__":
    main()
