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
            
        # HDFS Configuration
        self.hdfs_enabled = os.getenv("HDFS_ENABLED", "false").lower() == "true"
        self.hdfs_url = os.getenv("HDFS_URL", "http://namenode:9870")
        self.hdfs_user = os.getenv("HDFS_USER", "root")
        self.hdfs_client = None
        self.hdfs_lock = asyncio.Lock()
        
        if self.hdfs_enabled:
            try:
                from hdfs import InsecureClient
                self.hdfs_client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
                logger.info(f"HDFS Data Lake integration enabled pointing to {self.hdfs_url}")
            except Exception as e:
                logger.error(f"Failed to initialize HDFS client: {e}. HDFS storage disabled.")
                self.hdfs_enabled = False

    async def start(self):
        self.consumer.subscribe([KAFKA_TOPIC])
        self.running = True
        logger.info(f"Kafka Consumer started, subscribed to {KAFKA_TOPIC}")
        
        # Start background HDFS sync loop
        sync_task = None
        if self.hdfs_enabled:
            sync_task = asyncio.create_task(self.hdfs_sync_loop())
        
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
            if sync_task:
                sync_task.cancel()
                try:
                    await sync_task
                except asyncio.CancelledError:
                    pass

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
            
            # Store Validated Clean Data to Silver Data Lake
            await self.store_silver_datalake(raw_payload)
            
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
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Local Storage (Bronze Tier)
        local_filepath = os.path.join(DATA_LAKE_PATH, "bronze", f"raw_events_{today_str}.jsonl")
        def write_local():
            with open(local_filepath, 'a') as f:
                f.write(json.dumps(payload) + '\n')
        await asyncio.to_thread(write_local)

    async def store_silver_datalake(self, payload):
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Local Storage (Silver Tier)
        local_filepath = os.path.join(DATA_LAKE_PATH, "silver", f"clean_events_{today_str}.jsonl")
        def write_local_silver():
            with open(local_filepath, 'a') as f:
                f.write(json.dumps(payload) + '\n')
        await asyncio.to_thread(write_local_silver)

    async def hdfs_sync_loop(self):
        logger.info("HDFS Data Lake sync loop started.")
        while self.running:
            try:
                # Wait 10 seconds between syncs
                await asyncio.sleep(10)
                if self.hdfs_enabled and self.hdfs_client:
                    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    
                    # 1. Compile Gold KPIs from database
                    gold_kpis = {}
                    async with AsyncSessionLocal() as session:
                        try:
                            # Query live statistics from v_live_sensor_stats view
                            stats_res = await session.execute(text("SELECT * FROM v_live_sensor_stats"))
                            zone_stats = []
                            for row in stats_res:
                                zone_stats.append({
                                    "zone": row.zone,
                                    "active_sensors": row.active_sensors,
                                    "total_events_1h": row.total_events_1h,
                                    "avg_temperature": float(row.avg_temperature) if row.avg_temperature else None,
                                    "max_temperature": float(row.max_temperature) if row.max_temperature else None
                                })
                            
                            # Query anomalies metrics
                            anomaly_res = await session.execute(text(
                                "SELECT severity, count(*) as count, "
                                "sum(case when is_resolved then 1 else 0 end) as resolved "
                                "FROM anomalies GROUP BY severity"
                            ))
                            anomalies_summary = []
                            for row in anomaly_res:
                                anomalies_summary.append({
                                    "severity": row.severity,
                                    "count": row.count,
                                    "resolved": int(row.resolved) if row.resolved else 0
                                })
                            
                            # Query total and flagged containers
                            container_res = await session.execute(text(
                                "SELECT count(*) as total, sum(case when is_flagged then 1 else 0 end) as flagged FROM containers"
                            ))
                            c_row = container_res.fetchone()
                            containers_summary = {
                                "total_containers": c_row.total if c_row else 0,
                                "flagged_containers": int(c_row.flagged) if c_row and c_row.flagged else 0
                            }
                            
                            gold_kpis = {
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "zone_statistics": zone_stats,
                                "anomalies_summary": anomalies_summary,
                                "containers_summary": containers_summary
                            }
                        except Exception as e:
                            logger.error(f"Error compiling Gold KPIs from DB: {e}")

                    # Write Gold KPIs locally
                    if gold_kpis:
                        local_gold_filepath = os.path.join(DATA_LAKE_PATH, "gold", f"kpi_summary_{today_str}.json")
                        def write_local_gold():
                            with open(local_gold_filepath, 'w') as f:
                                json.dump(gold_kpis, f, indent=4)
                        await asyncio.to_thread(write_local_gold)

                    # 2. Sync Bronze to HDFS
                    local_bronze = os.path.join(DATA_LAKE_PATH, "bronze", f"raw_events_{today_str}.jsonl")
                    if os.path.exists(local_bronze):
                        hdfs_bronze_dir = "/datalake/bronze"
                        hdfs_bronze_path = f"{hdfs_bronze_dir}/raw_events_{today_str}.jsonl"
                        def sync_bronze():
                            try:
                                self.hdfs_client.makedirs(hdfs_bronze_dir)
                                self.hdfs_client.upload(hdfs_bronze_path, local_bronze, overwrite=True)
                            except Exception as e:
                                logger.error(f"Error syncing Bronze to HDFS: {e}")
                        await asyncio.to_thread(sync_bronze)
                        logger.info(f"HDFS Sync: Bronze Tier updated -> {hdfs_bronze_path}")

                    # 3. Sync Silver to HDFS
                    local_silver = os.path.join(DATA_LAKE_PATH, "silver", f"clean_events_{today_str}.jsonl")
                    if os.path.exists(local_silver):
                        hdfs_silver_dir = "/datalake/silver"
                        hdfs_silver_path = f"{hdfs_silver_dir}/clean_events_{today_str}.jsonl"
                        def sync_silver():
                            try:
                                self.hdfs_client.makedirs(hdfs_silver_dir)
                                self.hdfs_client.upload(hdfs_silver_path, local_silver, overwrite=True)
                            except Exception as e:
                                logger.error(f"Error syncing Silver to HDFS: {e}")
                        await asyncio.to_thread(sync_silver)
                        logger.info(f"HDFS Sync: Silver Tier updated -> {hdfs_silver_path}")

                    # 4. Sync Gold to HDFS
                    local_gold = os.path.join(DATA_LAKE_PATH, "gold", f"kpi_summary_{today_str}.json")
                    if os.path.exists(local_gold):
                        hdfs_gold_dir = "/datalake/gold"
                        hdfs_gold_path = f"{hdfs_gold_dir}/kpi_summary_{today_str}.json"
                        def sync_gold():
                            try:
                                self.hdfs_client.makedirs(hdfs_gold_dir)
                                self.hdfs_client.upload(hdfs_gold_path, local_gold, overwrite=True)
                            except Exception as e:
                                logger.error(f"Error syncing Gold to HDFS: {e}")
                        await asyncio.to_thread(sync_gold)
                        logger.info(f"HDFS Sync: Gold Tier updated -> {hdfs_gold_path}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in HDFS sync loop: {e}")

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

        # 2. Update/Insert Sensor dynamically (Auto-register new sensors from simulator)
        sens_id = data.sensor_id
        sens_type = 'COMBINED'
        if 'GPS' in sens_id:
            sens_type = 'GPS'
        elif 'TEMP' in sens_id:
            sens_type = 'TEMPERATURE'
        elif 'MOV' in sens_id:
            sens_type = 'MOVEMENT'
        elif 'EQP' in sens_id:
            sens_type = 'EQUIPMENT_STATUS'

        await session.execute(text("""
            INSERT INTO sensors (sensor_id, sensor_type, zone, last_seen, is_active)
            VALUES (:sens_id, :sens_type, :zone, :ts, TRUE)
            ON CONFLICT (sensor_id) DO UPDATE SET
                last_seen = EXCLUDED.last_seen,
                zone = EXCLUDED.zone,
                is_active = TRUE
        """), {
            "sens_id": sens_id,
            "sens_type": sens_type,
            "zone": data.zone,
            "ts": data.timestamp
        })

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
