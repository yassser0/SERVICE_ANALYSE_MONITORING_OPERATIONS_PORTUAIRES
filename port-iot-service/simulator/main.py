import os
import time
import json
import uuid
import random
from datetime import datetime, timezone
import logging
from confluent_kafka import Producer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "port-iot-data")
EMIT_INTERVAL_SECONDS = int(os.getenv("EMIT_INTERVAL_SECONDS", "3"))
NUM_SENSORS = int(os.getenv("NUM_SENSORS", "20"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Setup Logging
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Simulator")

# Static data for simulation
ZONES = ['QUAY_A', 'QUAY_B', 'QUAY_C', 'YARD_NORTH', 'YARD_SOUTH', 'GATE_ENTRY', 'GATE_EXIT', 'WAREHOUSE_1', 'WAREHOUSE_2', 'MAINTENANCE_AREA']
EQUIPMENT_STATUSES = ['ACTIVE', 'ACTIVE', 'ACTIVE', 'IDLE', 'MAINTENANCE', 'ERROR']
CONTAINERS = [f"CMAU{str(random.randint(1000000, 9999999))}" for _ in range(50)]
SENSORS = [
    {"sensor_id": f"SNS-GPS-{i:03d}", "type": "GPS"} for i in range(1, 5)
] + [
    {"sensor_id": f"SNS-TEMP-{i:03d}", "type": "TEMPERATURE"} for i in range(1, 5)
] + [
    {"sensor_id": f"SNS-MOV-{i:03d}", "type": "MOVEMENT"} for i in range(1, 5)
] + [
    {"sensor_id": f"SNS-EQP-{i:03d}", "type": "EQUIPMENT_STATUS"} for i in range(1, 5)
] + [
    {"sensor_id": f"SNS-CMB-{i:03d}", "type": "COMBINED"} for i in range(1, 5)
]

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_sensor_data(sensor):
    base_data = {
        "event_id": str(uuid.uuid4()),
        "sensor_id": sensor["sensor_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "zone": random.choice(ZONES),
        "container_id": random.choice(CONTAINERS) if random.random() > 0.3 else None
    }
    
    # Introduce anomalies randomly (e.g., ~5% chance for some fields)
    is_anomaly = random.random() < 0.05
    
    # Coordinates for Port (roughly)
    # Lat: 33.5 to 34.0, Lon: -8.0 to -7.5
    lat = round(random.uniform(33.5, 34.0), 6)
    lon = round(random.uniform(-8.0, -7.5), 6)
    
    if sensor["type"] in ["GPS", "COMBINED", "MOVEMENT"]:
        if is_anomaly and random.random() < 0.5:
            # Invalid GPS
            base_data["latitude"] = 99.99
            base_data["longitude"] = -199.99
        else:
            base_data["latitude"] = lat
            base_data["longitude"] = lon
            
    if sensor["type"] in ["TEMPERATURE", "COMBINED"]:
        if is_anomaly:
            # High Temperature
            base_data["temperature"] = round(random.uniform(85.0, 105.0), 2)
        else:
            base_data["temperature"] = round(random.uniform(18.0, 30.0), 2)
            
    if sensor["type"] in ["EQUIPMENT_STATUS", "COMBINED", "MOVEMENT"]:
        if is_anomaly and sensor["type"] == "EQUIPMENT_STATUS":
            base_data["equipment_status"] = "ERROR"
        else:
            base_data["equipment_status"] = random.choice(EQUIPMENT_STATUSES)

    return base_data

def main():
    logger.info(f"Starting IoT Simulator. Target Kafka: {KAFKA_BOOTSTRAP_SERVERS}, Topic: {KAFKA_TOPIC}")
    
    # Configure Kafka Producer
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'iot-simulator'
    }
    
    # Try connecting with retries
    producer = None
    retries = 10
    while retries > 0:
        try:
            producer = Producer(conf)
            logger.info("Successfully connected to Kafka.")
            break
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            retries -= 1
            time.sleep(5)
            
    if not producer:
        logger.error("Could not connect to Kafka. Exiting.")
        return

    try:
        while True:
            # Select a batch of sensors to emit data
            num_emit = random.randint(1, min(NUM_SENSORS, len(SENSORS)))
            sensors_to_emit = random.sample(SENSORS, num_emit)
            
            for sensor in sensors_to_emit:
                payload = generate_sensor_data(sensor)
                producer.produce(
                    KAFKA_TOPIC, 
                    key=payload["sensor_id"], 
                    value=json.dumps(payload), 
                    callback=delivery_report
                )
            
            producer.poll(0)
            logger.info(f"Emitted {num_emit} sensor events.")
            time.sleep(EMIT_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("Simulator stopped by user.")
    finally:
        logger.info("Flushing messages...")
        producer.flush()

if __name__ == "__main__":
    main()
