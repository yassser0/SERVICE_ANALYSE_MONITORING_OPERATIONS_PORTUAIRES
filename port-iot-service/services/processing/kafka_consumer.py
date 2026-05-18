import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, KafkaException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from database import AsyncSessionLocal
from models import SensorEvent, Anomaly, Container, Sensor
from schemas import SensorDataPayload
from pydantic import ValidationError
import pandas as pd
from uuid import UUID

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "port-iot-data")
DATA_LAKE_PATH = os.getenv("DATA_LAKE_PATH", "/data-lake")

TEMP_HIGH_THRESHOLD = float(os.getenv("TEMP_HIGH_THRESHOLD", "85.0"))
GPS_LAT_MIN = float(os.getenv("GPS_LAT_MIN", "33.0"))
GPS_LAT_MAX = float(os.getenv("GPS_LAT_MAX", "34.5"))
GPS_LON_MIN = float(os.getenv("GPS_LON_MIN", " -8.5"))
GPS_LON_MAX = float(os.getenv("GPS_LON_MAX", " -7.0"))

class KafkaIoTConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'fastapi-processing-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        self.consumer = Consumer(self.conf)
        self.running = False
        self.batch_size = 100
        
        # Ensure Data Lake directories exist
        for tier in ['bronze', 'silver', 'gold']:
            os.makedirs(os.path.join(DATA_LAKE_PATH, tier), exist_ok=True)

    async def start(self):
        self.consumer.subscribe([KAFKA_TOPIC])
        self.running = True
        logger.info(f"Kafka Consumer started, subscribed to {KAFKA_TOPIC}")
        
        try:
            while self.running:
                # Use a background thread for blocking poll
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.error(f"Kafka error: {msg.error()}")
                        continue
                
                await self.process_message(msg)
                
        except asyncio.CancelledError:
            logger.info("Kafka Consumer task cancelled.")
        except Exception as e:
            logger.error(f"Error in Kafka Consumer loop: {e}")
        finally:
            self.consumer.close()
            logger.info("Kafka Consumer closed.")

    def stop(self):
        self.running = False

    async def process_message(self, msg):
        try:
            val = msg.value().decode('utf-8')
            raw_payload = json.loads(val)
            
            # 1. Store Raw to Bronze Data Lake (simplified append)
            await self.store_bronze_datalake(raw_payload)
            
            # 2. Validate payload
            payload = SensorDataPayload(**raw_payload)
            
            # 3. Detect Anomalies & Update State
            async with AsyncSessionLocal() as session:
                await self.handle_business_logic(session, payload, raw_payload)
                await session.commit()
            
            # Commit offset after successful processing
            self.consumer.commit(asynchronous=True)
            
        except ValidationError as e:
            logger.warning(f"Invalid message format: {e}")
            self.consumer.commit(asynchronous=True) # Commit anyway to avoid poison pills
        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    async def store_bronze_datalake(self, payload):
        # In a real system, this would be batched and stored as Parquet
        # For simplicity, we just append to a daily JSONL file
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        filepath = os.path.join(DATA_LAKE_PATH, "bronze", f"raw_events_{today_str}.jsonl")
        
        def write_file():
            with open(filepath, 'a') as f:
                f.write(json.dumps(payload) + '\n')
                
        await asyncio.to_thread(write_file)

    async def handle_business_logic(self, session: AsyncSession, data: SensorDataPayload, raw_payload: dict):
        anomalies = []
        is_anomaly = False
        
        # Check Temperature
        if data.temperature is not None and data.temperature > TEMP_HIGH_THRESHOLD:
            anomalies.append({
                "type": "HIGH_TEMPERATURE",
                "severity": "CRITICAL",
                "desc": f"Temperature {data.temperature} exceeds threshold {TEMP_HIGH_THRESHOLD}",
                "val": data.temperature,
                "thresh": TEMP_HIGH_THRESHOLD
            })
            is_anomaly = True

        # Check GPS
        if data.latitude is not None and data.longitude is not None:
            if not (GPS_LAT_MIN <= data.latitude <= GPS_LAT_MAX) or not (GPS_LON_MIN <= data.longitude <= GPS_LON_MAX):
                anomalies.append({
                    "type": "INVALID_GPS",
                    "severity": "HIGH",
                    "desc": f"GPS ({data.latitude}, {data.longitude}) is outside port bounds.",
                    "val": float(data.latitude),
                    "thresh": None
                })
                is_anomaly = True

        # Check Equipment Status
        if data.equipment_status in ["ERROR", "OFFLINE"]:
            anomalies.append({
                "type": "EQUIPMENT_ERROR",
                "severity": "HIGH",
                "desc": f"Equipment reported status: {data.equipment_status}",
                "val": None,
                "thresh": None
            })
            is_anomaly = True

        event_uuid = UUID(data.event_id)
        
        # 1. Insert Event (Use raw SQL for partitioned table efficiency)
        stmt = text("""
            INSERT INTO sensor_events (event_id, sensor_id, container_id, temperature_celsius, zone, equipment_status, latitude, longitude, raw_payload, event_timestamp, is_anomaly)
            VALUES (:ev_id, :sens_id, :cont_id, :temp, :zone, :eqp, :lat, :lon, :raw, :ts, :is_anom)
        """)
        await session.execute(stmt, {
            "ev_id": event_uuid,
            "sens_id": data.sensor_id,
            "cont_id": data.container_id,
            "temp": data.temperature,
            "zone": data.zone,
            "eqp": data.equipment_status,
            "lat": data.latitude,
            "lon": data.longitude,
            "raw": json.dumps(raw_payload),
            "ts": data.timestamp,
            "is_anom": is_anomaly
        })

        # 2. Update Sensor Last Seen
        await session.execute(text("""
            UPDATE sensors 
            SET last_seen = :ts, zone = :zone 
            WHERE sensor_id = :sens_id
        """), {"ts": data.timestamp, "zone": data.zone, "sens_id": data.sensor_id})

        # 3. Update Container State if applicable
        if data.container_id:
            await session.execute(text("""
                INSERT INTO containers (container_id, current_zone, current_lat, current_lon, equipment_status, temperature_celsius, last_seen, is_flagged)
                VALUES (:cid, :zone, :lat, :lon, :eqp, :temp, :ts, :flag)
                ON CONFLICT (container_id) DO UPDATE SET
                    current_zone = EXCLUDED.current_zone,
                    current_lat = EXCLUDED.current_lat,
                    current_lon = EXCLUDED.current_lon,
                    equipment_status = EXCLUDED.equipment_status,
                    temperature_celsius = EXCLUDED.temperature_celsius,
                    last_seen = EXCLUDED.last_seen,
                    is_flagged = EXCLUDED.is_flagged OR containers.is_flagged
            """), {
                "cid": data.container_id,
                "zone": data.zone,
                "lat": data.latitude,
                "lon": data.longitude,
                "eqp": data.equipment_status or 'ACTIVE',
                "temp": data.temperature,
                "ts": data.timestamp,
                "flag": is_anomaly
            })

        # 4. Insert Anomalies
        for anom in anomalies:
            new_anomaly = Anomaly(
                event_id=event_uuid,
                sensor_id=data.sensor_id,
                container_id=data.container_id,
                anomaly_type=anom["type"],
                severity=anom["severity"],
                description=anom["desc"],
                detected_value=anom["val"],
                threshold_value=anom["thresh"],
                zone=data.zone,
                raw_payload=raw_payload,
                detected_at=data.timestamp
            )
            session.add(new_anomaly)
