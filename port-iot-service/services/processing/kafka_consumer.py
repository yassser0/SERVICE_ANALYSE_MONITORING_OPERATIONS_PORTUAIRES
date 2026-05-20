import os
import json
import logging
import asyncio
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from database import AsyncSessionLocal
from models import SensorEvent, Anomaly, Container, Sensor
from schemas import SensorDataPayload
from pydantic import ValidationError
from uuid import UUID
import math

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC              = os.getenv("KAFKA_TOPIC", "port-iot-data")
DATA_LAKE_PATH           = os.getenv("DATA_LAKE_PATH", "/data-lake")

TEMP_HIGH_THRESHOLD      = float(os.getenv("TEMP_HIGH_THRESHOLD", "85.0"))
GPS_LAT_MIN              = float(os.getenv("GPS_LAT_MIN", "33.0"))
GPS_LAT_MAX              = float(os.getenv("GPS_LAT_MAX", "34.5"))
GPS_LON_MIN              = float(os.getenv("GPS_LON_MIN", "-8.5"))
GPS_LON_MAX              = float(os.getenv("GPS_LON_MAX", "-7.0"))
SPEED_LIMIT_KMPH         = float(os.getenv("SPEED_LIMIT_KMPH", "25.0"))
BATTERY_LOW_THRESHOLD    = float(os.getenv("BATTERY_LOW_THRESHOLD", "15.0"))
SENSOR_OFFLINE_MINUTES   = int(os.getenv("SENSOR_OFFLINE_MINUTES", "5"))

# Zones where speed limits are strictly enforced
SPEED_LIMITED_ZONES = {
    'YARD_NORTH', 'YARD_SOUTH', 'WAREHOUSE_1', 'WAREHOUSE_2',
    'GATE_ENTRY', 'GATE_EXIT'
}

# ─── Haversine distance (metres) ─────────────────────────────────────────────

def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class KafkaIoTConsumer:
    def __init__(self):
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'fastapi-processing-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.consumer = Consumer(self.conf)
        self.running  = False

        # Ensure Data Lake tiers exist
        for tier in ('bronze', 'silver', 'gold'):
            os.makedirs(os.path.join(DATA_LAKE_PATH, tier), exist_ok=True)

        # HDFS
        self.hdfs_enabled = os.getenv("HDFS_ENABLED", "false").lower() == "true"
        self.hdfs_url     = os.getenv("HDFS_URL", "http://namenode:9870")
        self.hdfs_user    = os.getenv("HDFS_USER", "root")
        self.hdfs_client  = None
        if self.hdfs_enabled:
            try:
                from hdfs import InsecureClient
                self.hdfs_client = InsecureClient(self.hdfs_url, user=self.hdfs_user)
                logger.info(f"HDFS integration enabled → {self.hdfs_url}")
            except Exception as e:
                logger.error(f"Failed to init HDFS client: {e}. HDFS disabled.")
                self.hdfs_enabled = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        self.consumer.subscribe([KAFKA_TOPIC])
        self.running = True
        logger.info(f"Kafka Consumer started → topic '{KAFKA_TOPIC}'")

        tasks = []
        if self.hdfs_enabled:
            tasks.append(asyncio.create_task(self.hdfs_sync_loop()))
        tasks.append(asyncio.create_task(self._sensor_offline_check_loop()))

        try:
            while self.running:
                msg = await asyncio.to_thread(self.consumer.poll, timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue
                await self.process_message(msg)
        except asyncio.CancelledError:
            logger.info("Kafka Consumer cancelled.")
        except Exception as e:
            logger.error(f"Consumer loop error: {e}")
        finally:
            self.consumer.close()
            for t in tasks:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    def stop(self):
        self.running = False

    # ── Message Processing ────────────────────────────────────────────────────

    async def process_message(self, msg):
        try:
            raw_payload = json.loads(msg.value().decode('utf-8'))

            # 1. Bronze (raw)
            await self._append_datalake('bronze', f"raw_events_{self._today()}.jsonl", raw_payload)

            # 2. Validate
            payload = SensorDataPayload(**raw_payload)

            # 3. Silver (validated)
            await self._append_datalake('silver', f"clean_events_{self._today()}.jsonl", raw_payload)

            # 4. Business logic
            async with AsyncSessionLocal() as session:
                await self.handle_business_logic(session, payload, raw_payload)
                await session.commit()

            self.consumer.commit(asynchronous=True)

        except ValidationError as e:
            logger.warning(f"Invalid message: {e}")
            self.consumer.commit(asynchronous=True)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")

    # ── Business Logic & Anomaly Detection ───────────────────────────────────

    async def handle_business_logic(self, session: AsyncSession, data: SensorDataPayload, raw: dict):
        anomalies = []
        is_anomaly = False

        # 1. HIGH TEMPERATURE
        if data.temperature is not None and data.temperature > TEMP_HIGH_THRESHOLD:
            anomalies.append({
                "type": "HIGH_TEMPERATURE", "severity": "CRITICAL",
                "desc": f"Temperature {data.temperature}°C exceeds threshold {TEMP_HIGH_THRESHOLD}°C",
                "val": data.temperature, "thresh": TEMP_HIGH_THRESHOLD,
            })
            is_anomaly = True

        # 2. INVALID GPS
        if data.latitude is not None and data.longitude is not None:
            if not (GPS_LAT_MIN <= data.latitude <= GPS_LAT_MAX) or \
               not (GPS_LON_MIN <= data.longitude <= GPS_LON_MAX):
                anomalies.append({
                    "type": "INVALID_GPS", "severity": "HIGH",
                    "desc": f"GPS ({data.latitude}, {data.longitude}) outside port bounds.",
                    "val": float(data.latitude), "thresh": None,
                })
                is_anomaly = True

        # 3. EQUIPMENT ERROR
        if data.equipment_status in ("ERROR", "OFFLINE"):
            anomalies.append({
                "type": "EQUIPMENT_ERROR", "severity": "HIGH",
                "desc": f"Equipment reported status: {data.equipment_status}",
                "val": None, "thresh": None,
            })
            is_anomaly = True

        # 4. LOW BATTERY
        if data.battery_level is not None and data.battery_level < BATTERY_LOW_THRESHOLD:
            anomalies.append({
                "type": "LOW_BATTERY", "severity": "MEDIUM",
                "desc": f"Sensor {data.sensor_id} battery at {data.battery_level:.1f}% (threshold {BATTERY_LOW_THRESHOLD}%)",
                "val": data.battery_level, "thresh": BATTERY_LOW_THRESHOLD,
            })
            is_anomaly = True

        # 5. SPEED VIOLATION
        if data.speed_kmh is not None and data.zone in SPEED_LIMITED_ZONES:
            if data.speed_kmh > SPEED_LIMIT_KMPH:
                anomalies.append({
                    "type": "SPEED_VIOLATION", "severity": "HIGH",
                    "desc": f"Speed {data.speed_kmh} km/h exceeds limit {SPEED_LIMIT_KMPH} km/h in {data.zone}",
                    "val": data.speed_kmh, "thresh": SPEED_LIMIT_KMPH,
                })
                is_anomaly = True

        # 6. UNEXPECTED MOVEMENT (container moved > 200 m since last known position)
        if data.container_id and data.latitude is not None and data.longitude is not None:
            row = await session.execute(
                text("SELECT current_lat, current_lon FROM containers WHERE container_id = :cid"),
                {"cid": data.container_id}
            )
            prev = row.fetchone()
            if prev and prev.current_lat and prev.current_lon:
                dist_m = _haversine_m(
                    float(prev.current_lat), float(prev.current_lon),
                    data.latitude, data.longitude
                )
                if dist_m > 200:
                    anomalies.append({
                        "type": "UNEXPECTED_MOVEMENT", "severity": "HIGH",
                        "desc": f"Container {data.container_id} moved {dist_m:.0f} m unexpectedly.",
                        "val": round(dist_m, 1), "thresh": 200.0,
                    })
                    is_anomaly = True

        event_uuid = UUID(data.event_id)

        # ── Insert sensor_events (partitioned) ───────────────────────────────
        await session.execute(text("""
            INSERT INTO sensor_events (
                event_id, sensor_id, container_id, vessel_id,
                temperature_celsius, humidity, zone, equipment_status,
                latitude, longitude, speed_kmh, heading_degrees,
                battery_level, signal_strength, load_percentage,
                raw_payload, event_timestamp, is_anomaly
            ) VALUES (
                :ev_id, :sens_id, :cont_id, :vessel_id,
                :temp, :hum, :zone, :eqp,
                :lat, :lon, :spd, :hdg,
                :bat, :sig, :load,
                :raw, :ts, :is_anom
            )
        """), {
            "ev_id":    event_uuid,
            "sens_id":  data.sensor_id,
            "cont_id":  data.container_id,
            "vessel_id": data.vessel_id,
            "temp":     data.temperature,
            "hum":      data.humidity,
            "zone":     data.zone,
            "eqp":      data.equipment_status,
            "lat":      data.latitude,
            "lon":      data.longitude,
            "spd":      data.speed_kmh,
            "hdg":      data.heading_degrees,
            "bat":      data.battery_level,
            "sig":      data.signal_strength,
            "load":     data.load_percentage,
            "raw":      json.dumps(raw),
            "ts":       data.timestamp,
            "is_anom":  is_anomaly,
        })

        # ── Upsert sensor ─────────────────────────────────────────────────────
        sens_type = 'COMBINED'
        sid = data.sensor_id
        if 'GPS'  in sid: sens_type = 'GPS'
        elif 'TEMP' in sid: sens_type = 'TEMPERATURE'
        elif 'MOV'  in sid: sens_type = 'MOVEMENT'
        elif 'EQP'  in sid: sens_type = 'EQUIPMENT_STATUS'

        await session.execute(text("""
            INSERT INTO sensors (sensor_id, sensor_type, zone, last_seen, is_active)
            VALUES (:sid, :stype, :zone, :ts, TRUE)
            ON CONFLICT (sensor_id) DO UPDATE SET
                last_seen = EXCLUDED.last_seen,
                zone      = EXCLUDED.zone,
                is_active = TRUE
        """), {"sid": sid, "stype": sens_type, "zone": data.zone, "ts": data.timestamp})

        # ── Upsert container ──────────────────────────────────────────────────
        if data.container_id:
            await session.execute(text("""
                INSERT INTO containers (
                    container_id, vessel_id, current_zone,
                    current_lat, current_lon,
                    equipment_status, temperature_celsius,
                    last_seen, is_flagged
                ) VALUES (
                    :cid, :vessel_id, :zone,
                    :lat, :lon,
                    :eqp, :temp,
                    :ts, :flag
                )
                ON CONFLICT (container_id) DO UPDATE SET
                    vessel_id           = COALESCE(EXCLUDED.vessel_id, containers.vessel_id),
                    current_zone        = EXCLUDED.current_zone,
                    prev_lat            = containers.current_lat,
                    prev_lon            = containers.current_lon,
                    current_lat         = EXCLUDED.current_lat,
                    current_lon         = EXCLUDED.current_lon,
                    equipment_status    = EXCLUDED.equipment_status,
                    temperature_celsius = EXCLUDED.temperature_celsius,
                    last_seen           = EXCLUDED.last_seen,
                    is_flagged          = EXCLUDED.is_flagged OR containers.is_flagged
            """), {
                "cid":       data.container_id,
                "vessel_id": data.vessel_id,
                "zone":      data.zone,
                "lat":       data.latitude,
                "lon":       data.longitude,
                "eqp":       data.equipment_status or 'ACTIVE',
                "temp":      data.temperature,
                "ts":        data.timestamp,
                "flag":      is_anomaly,
            })

        # ── Insert anomalies ──────────────────────────────────────────────────
        for anom in anomalies:
            session.add(Anomaly(
                event_id      = event_uuid,
                sensor_id     = data.sensor_id,
                container_id  = data.container_id,
                anomaly_type  = anom["type"],
                severity      = anom["severity"],
                description   = anom["desc"],
                detected_value= anom["val"],
                threshold_value=anom["thresh"],
                zone          = data.zone,
                raw_payload   = raw,
                detected_at   = data.timestamp,
            ))

    # ── Sensor Offline Check (background loop) ────────────────────────────────

    async def _sensor_offline_check_loop(self):
        """Every 60 s check for sensors that haven't sent a heartbeat in SENSOR_OFFLINE_MINUTES."""
        logger.info("Sensor offline check loop started.")
        while self.running:
            try:
                await asyncio.sleep(60)
                async with AsyncSessionLocal() as session:
                    offline = await session.execute(text(f"""
                        SELECT sensor_id, zone, last_seen
                        FROM sensors
                        WHERE is_active = TRUE
                          AND last_seen < NOW() - INTERVAL '{SENSOR_OFFLINE_MINUTES} minutes'
                    """))
                    for row in offline:
                        # Flag the sensor as inactive and create an anomaly
                        await session.execute(text("""
                            UPDATE sensors SET is_active = FALSE WHERE sensor_id = :sid
                        """), {"sid": row.sensor_id})
                        session.add(Anomaly(
                            sensor_id    = row.sensor_id,
                            anomaly_type = 'SENSOR_OFFLINE',
                            severity     = 'HIGH',
                            description  = (
                                f"Sensor {row.sensor_id} in zone {row.zone} "
                                f"has not reported for over {SENSOR_OFFLINE_MINUTES} minutes."
                            ),
                            zone         = row.zone,
                            detected_at  = datetime.now(timezone.utc),
                        ))
                    await session.commit()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sensor offline check: {e}")

    # ── Data Lake Helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _today() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    async def _append_datalake(self, tier: str, filename: str, payload: dict):
        path = os.path.join(DATA_LAKE_PATH, tier, filename)
        line = json.dumps(payload) + '\n'
        def _write():
            with open(path, 'a') as f:
                f.write(line)
        await asyncio.to_thread(_write)

    # ── HDFS Sync Loop ────────────────────────────────────────────────────────

    async def hdfs_sync_loop(self):
        logger.info("HDFS sync loop started.")
        while self.running:
            try:
                await asyncio.sleep(10)
                if not (self.hdfs_enabled and self.hdfs_client):
                    continue
                today = self._today()
                gold_kpis = {}

                async with AsyncSessionLocal() as session:
                    try:
                        stats_res = await session.execute(text("SELECT * FROM v_live_sensor_stats"))
                        zone_stats = [
                            {
                                "zone": r.zone,
                                "active_sensors": r.active_sensors,
                                "total_events_1h": r.total_events_1h,
                                "avg_temperature": float(r.avg_temperature) if r.avg_temperature else None,
                                "max_temperature": float(r.max_temperature) if r.max_temperature else None,
                                "avg_battery":     float(r.avg_battery_level) if r.avg_battery_level else None,
                            }
                            for r in stats_res
                        ]
                        anomaly_res = await session.execute(text(
                            "SELECT severity, count(*) AS count, "
                            "sum(CASE WHEN is_resolved THEN 1 ELSE 0 END) AS resolved "
                            "FROM anomalies GROUP BY severity"
                        ))
                        c_row = (await session.execute(text(
                            "SELECT count(*) AS total, sum(CASE WHEN is_flagged THEN 1 ELSE 0 END) AS flagged FROM containers"
                        ))).fetchone()
                        gold_kpis = {
                            "timestamp":          datetime.now(timezone.utc).isoformat(),
                            "zone_statistics":    zone_stats,
                            "anomalies_summary":  [
                                {"severity": r.severity, "count": r.count,
                                 "resolved": int(r.resolved) if r.resolved else 0}
                                for r in anomaly_res
                            ],
                            "containers_summary": {
                                "total_containers":   c_row.total if c_row else 0,
                                "flagged_containers": int(c_row.flagged) if c_row and c_row.flagged else 0,
                            },
                        }
                    except Exception as e:
                        logger.error(f"Error compiling Gold KPIs: {e}")

                if gold_kpis:
                    await self._append_datalake('gold', f"kpi_summary_{today}.json",
                                                gold_kpis)  # type: ignore

                for tier, filename in [
                    ('bronze', f"raw_events_{today}.jsonl"),
                    ('silver', f"clean_events_{today}.jsonl"),
                    ('gold',   f"kpi_summary_{today}.json"),
                ]:
                    local = os.path.join(DATA_LAKE_PATH, tier, filename)
                    if not os.path.exists(local):
                        continue
                    hdfs_dir  = f"/datalake/{tier}"
                    hdfs_path = f"{hdfs_dir}/{filename}"
                    def _sync(lp=local, hp=hdfs_path, hd=hdfs_dir):
                        try:
                            self.hdfs_client.makedirs(hd)
                            self.hdfs_client.upload(hp, lp, overwrite=True)
                        except Exception as ex:
                            logger.error(f"HDFS sync error ({tier}): {ex}")
                    await asyncio.to_thread(_sync)
                    logger.info(f"HDFS sync: {tier} → {hdfs_path}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"HDFS sync loop error: {e}")
