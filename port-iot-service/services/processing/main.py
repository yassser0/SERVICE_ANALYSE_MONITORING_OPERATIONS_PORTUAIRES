import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Depends, Query, HTTPException, Path, Request, Response
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, text
from typing import List, Optional

from database import get_db, AsyncSessionLocal
from models import Container, Sensor, Anomaly, Vessel
from schemas import (
    ContainerResponse, SensorResponse, AnomalyResponse,
    VesselResponse, ResolveAnomalyRequest,
)
from kafka_consumer import KafkaIoTConsumer

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FastAPI")

kafka_consumer = KafkaIoTConsumer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Port IoT Processing Service...")
    consumer_task = asyncio.create_task(kafka_consumer.start())
    yield
    logger.info("Shutting down Port IoT Processing Service...")
    kafka_consumer.stop()
    await consumer_task

app = FastAPI(
    title="Port IoT Data Service API",
    version="2.0",
    description="Real-time IoT monitoring platform for smart port operations.",
    lifespan=lifespan,
)

# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    return {"status": "healthy", "kafka_consumer_running": kafka_consumer.running}

# ─── Sensors ──────────────────────────────────────────────────────────────────

@app.get("/sensors", response_model=List[SensorResponse], tags=["Sensors"])
async def get_sensors(
    zone: Optional[str] = None,
    is_active: Optional[bool] = None,
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Sensor).order_by(Sensor.sensor_id).limit(limit)
    if zone:
        stmt = stmt.where(Sensor.zone == zone)
    if is_active is not None:
        stmt = stmt.where(Sensor.is_active == is_active)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/sensors/health", tags=["Sensors"])
async def get_sensor_health(db: AsyncSession = Depends(get_db)):
    """Battery levels, signal strength, and heartbeat age for all sensors."""
    result = await db.execute(text("SELECT * FROM v_sensor_health ORDER BY minutes_since_last DESC NULLS LAST"))
    rows = []
    for r in result:
        rows.append({
            "sensor_id":          r.sensor_id,
            "sensor_type":        r.sensor_type,
            "zone":               r.zone,
            "manufacturer":       r.manufacturer,
            "model":              r.model,
            "avg_battery":        float(r.avg_battery) if r.avg_battery else None,
            "min_battery":        float(r.min_battery) if r.min_battery else None,
            "avg_signal_dbm":     float(r.avg_signal)  if r.avg_signal  else None,
            "last_heartbeat":     r.last_heartbeat.isoformat() if r.last_heartbeat else None,
            "minutes_since_last": float(r.minutes_since_last) if r.minutes_since_last else None,
            "events_last_hour":   r.events_last_hour,
            "is_active":          r.is_active,
        })
    return {"data": rows}

# ─── Containers ───────────────────────────────────────────────────────────────

@app.get("/containers", response_model=List[ContainerResponse], tags=["Containers"])
async def get_containers(
    zone: Optional[str] = None,
    is_flagged: Optional[bool] = None,
    vessel_id: Optional[str] = None,
    limit: int = Query(50, le=1000),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Container).order_by(desc(Container.last_seen)).limit(limit)
    if zone:
        stmt = stmt.where(Container.current_zone == zone)
    if is_flagged is not None:
        stmt = stmt.where(Container.is_flagged == is_flagged)
    if vessel_id:
        stmt = stmt.where(Container.vessel_id == vessel_id)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/containers/{container_id}/history", tags=["Containers"])
async def get_container_history(
    container_id: str = Path(..., description="Container ID e.g. CMAU1234567"),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
):
    """Last N sensor events for a specific container."""
    result = await db.execute(text("""
        SELECT event_id, sensor_id, zone, temperature_celsius, humidity,
               latitude, longitude, speed_kmh, battery_level,
               equipment_status, is_anomaly, event_timestamp
        FROM sensor_events
        WHERE container_id = :cid
        ORDER BY event_timestamp DESC
        LIMIT :lim
    """), {"cid": container_id, "lim": limit})
    rows = []
    for r in result:
        rows.append({
            "event_id":           str(r.event_id) if r.event_id else None,
            "sensor_id":          r.sensor_id,
            "zone":               r.zone,
            "temperature_celsius":float(r.temperature_celsius) if r.temperature_celsius else None,
            "humidity":           float(r.humidity) if r.humidity else None,
            "latitude":           float(r.latitude) if r.latitude else None,
            "longitude":          float(r.longitude) if r.longitude else None,
            "speed_kmh":          float(r.speed_kmh) if r.speed_kmh else None,
            "battery_level":      float(r.battery_level) if r.battery_level else None,
            "equipment_status":   r.equipment_status,
            "is_anomaly":         r.is_anomaly,
            "event_timestamp":    r.event_timestamp.isoformat() if r.event_timestamp else None,
        })
    if not rows:
        raise HTTPException(status_code=404, detail=f"No events found for container '{container_id}'")
    return {"container_id": container_id, "count": len(rows), "events": rows}

# ─── Anomalies ────────────────────────────────────────────────────────────────

@app.get("/anomalies", response_model=List[AnomalyResponse], tags=["Anomalies"])
async def get_anomalies(
    severity: Optional[str] = None,
    is_resolved: Optional[bool] = None,
    anomaly_type: Optional[str] = None,
    limit: int = Query(50, le=1000),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Anomaly).order_by(desc(Anomaly.detected_at)).limit(limit)
    if severity:
        stmt = stmt.where(Anomaly.severity == severity)
    if is_resolved is not None:
        stmt = stmt.where(Anomaly.is_resolved == is_resolved)
    if anomaly_type:
        stmt = stmt.where(Anomaly.anomaly_type == anomaly_type)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.post("/anomalies/{anomaly_id}/resolve", tags=["Anomalies"])
async def resolve_anomaly(
    anomaly_id: str = Path(..., description="UUID of the anomaly"),
    body: ResolveAnomalyRequest = ...,
    db: AsyncSession = Depends(get_db),
):
    """Mark an anomaly as resolved."""
    result = await db.execute(text(
        "SELECT id FROM anomalies WHERE id = :aid"
    ), {"aid": anomaly_id})
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Anomaly not found")

    await db.execute(text("""
        UPDATE anomalies
        SET is_resolved = TRUE,
            resolved_at = :now,
            resolved_by = :by
        WHERE id = :aid
    """), {"aid": anomaly_id, "now": datetime.now(timezone.utc), "by": body.resolved_by})
    await db.commit()
    return {"status": "resolved", "anomaly_id": anomaly_id, "resolved_by": body.resolved_by}

# ─── Vessels ──────────────────────────────────────────────────────────────────

@app.get("/vessels", response_model=List[VesselResponse], tags=["Vessels"])
async def get_vessels(
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """List all vessels currently tracked in the port."""
    stmt = select(Vessel).order_by(Vessel.arrival_time)
    if status:
        stmt = stmt.where(Vessel.status == status)
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/vessels/{vessel_id}/containers", response_model=List[ContainerResponse], tags=["Vessels"])
async def get_vessel_containers(
    vessel_id: str = Path(..., description="Vessel ID e.g. MSC-AURORA"),
    db: AsyncSession = Depends(get_db),
):
    """All containers linked to a specific vessel."""
    stmt = select(Container).where(Container.vessel_id == vessel_id).order_by(desc(Container.last_seen))
    result = await db.execute(stmt)
    containers = result.scalars().all()
    if not containers:
        raise HTTPException(status_code=404, detail=f"No containers found for vessel '{vessel_id}'")
    return containers

# ─── Statistics & KPIs ────────────────────────────────────────────────────────

@app.get("/statistics", tags=["Statistics"])
async def get_statistics(db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(text("SELECT * FROM v_live_sensor_stats"))
        stats = []
        for r in result:
            stats.append({
                "zone":            r.zone,
                "active_sensors":  r.active_sensors,
                "total_events_1h": r.total_events_1h,
                "avg_temperature": float(r.avg_temperature) if r.avg_temperature else None,
                "max_temperature": float(r.max_temperature) if r.max_temperature else None,
                "avg_battery":     float(r.avg_battery_level) if r.avg_battery_level else None,
                "anomaly_count":   r.anomaly_count,
            })
        return {"data": stats}
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/kpis", tags=["Statistics"])
async def get_kpis(db: AsyncSession = Depends(get_db)):
    try:
        container_count  = (await db.execute(text("SELECT COUNT(*) FROM containers"))).scalar()
        anomaly_count    = (await db.execute(text("SELECT COUNT(*) FROM anomalies WHERE NOT is_resolved"))).scalar()
        sensor_count     = (await db.execute(text("SELECT COUNT(*) FROM sensors WHERE is_active = TRUE"))).scalar()
        vessel_count     = (await db.execute(text("SELECT COUNT(*) FROM vessels"))).scalar()
        flagged_count    = (await db.execute(text("SELECT COUNT(*) FROM containers WHERE is_flagged = TRUE"))).scalar()
        return {
            "total_containers":   container_count,
            "open_anomalies":     anomaly_count,
            "active_sensors":     sensor_count,
            "vessels_docked":     vessel_count,
            "flagged_containers": flagged_count,
        }
    except Exception as e:
        logger.error(f"Error fetching KPIs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

class LoginRequest(BaseModel):
    username: str
    password: str

# ─── Static Dashboard & Auth ──────────────────────────────────────────────────

static_dir = os.path.join(os.path.dirname(__file__), "static")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "PortSecure2024!")
SESSION_TOKEN = "admin_secret_token"

@app.post("/api/login", tags=["Auth"])
async def login(body: LoginRequest, response: Response):
    if body.username == ADMIN_USER and body.password == ADMIN_PASS:
        response.set_cookie(key="session_token", value=SESSION_TOKEN, httponly=True)
        return {"status": "success"}
    raise HTTPException(status_code=401, detail="Invalid credentials")

@app.post("/api/logout", tags=["Auth"])
async def logout(response: Response):
    response.delete_cookie("session_token")
    return {"status": "success"}

@app.get("/login", tags=["Dashboard"])
async def get_login_page():
    login_path = os.path.join(static_dir, "login.html")
    if os.path.exists(login_path):
        return FileResponse(login_path)
    raise HTTPException(status_code=404, detail="Login page not found")

@app.get("/", tags=["Dashboard"])
async def get_dashboard(request: Request):
    if request.cookies.get("session_token") != SESSION_TOKEN:
        return RedirectResponse(url="/login")
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="Dashboard not found")
