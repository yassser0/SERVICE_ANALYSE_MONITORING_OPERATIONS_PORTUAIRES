import asyncio
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc
from typing import List

from database import get_db, AsyncSessionLocal
from models import Container, Sensor, Anomaly
from schemas import ContainerResponse, SensorResponse, AnomalyResponse
from kafka_consumer import KafkaIoTConsumer

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FastAPI")

kafka_consumer = KafkaIoTConsumer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Port IoT Processing Service...")
    consumer_task = asyncio.create_task(kafka_consumer.start())
    yield
    # Shutdown
    logger.info("Shutting down Port IoT Processing Service...")
    kafka_consumer.stop()
    await consumer_task

app = FastAPI(title="Port IoT Data Service API", version="1.0", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "kafka_consumer_running": kafka_consumer.running}

@app.get("/containers", response_model=List[ContainerResponse])
async def get_containers(
    zone: str = None, 
    is_flagged: bool = None,
    limit: int = Query(50, le=1000), 
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Container).order_by(desc(Container.last_seen)).limit(limit)
    if zone:
        stmt = stmt.where(Container.current_zone == zone)
    if is_flagged is not None:
        stmt = stmt.where(Container.is_flagged == is_flagged)
        
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/sensors", response_model=List[SensorResponse])
async def get_sensors(
    zone: str = None,
    is_active: bool = None,
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Sensor).order_by(Sensor.sensor_id).limit(limit)
    if zone:
        stmt = stmt.where(Sensor.zone == zone)
    if is_active is not None:
        stmt = stmt.where(Sensor.is_active == is_active)
        
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/anomalies", response_model=List[AnomalyResponse])
async def get_anomalies(
    severity: str = None,
    is_resolved: bool = None,
    limit: int = Query(50, le=1000),
    db: AsyncSession = Depends(get_db)
):
    stmt = select(Anomaly).order_by(desc(Anomaly.detected_at)).limit(limit)
    if severity:
        stmt = stmt.where(Anomaly.severity == severity)
    if is_resolved is not None:
        stmt = stmt.where(Anomaly.is_resolved == is_resolved)
        
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/statistics")
async def get_statistics(db: AsyncSession = Depends(get_db)):
    # Using the Grafana view created in init.sql for live stats
    try:
        from sqlalchemy import text
        result = await db.execute(text("SELECT * FROM v_live_sensor_stats"))
        stats = []
        for row in result:
            stats.append({
                "zone": row.zone,
                "active_sensors": row.active_sensors,
                "total_events_1h": row.total_events_1h,
                "avg_temperature": float(row.avg_temperature) if row.avg_temperature else None,
                "max_temperature": float(row.max_temperature) if row.max_temperature else None,
                "anomaly_count": row.anomaly_count
            })
        return {"data": stats}
    except Exception as e:
        logger.error(f"Error fetching statistics: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

@app.get("/kpis")
async def get_kpis(db: AsyncSession = Depends(get_db)):
    from sqlalchemy import text
    try:
        container_count = (await db.execute(text("SELECT COUNT(*) FROM containers"))).scalar()
        anomaly_count = (await db.execute(text("SELECT COUNT(*) FROM anomalies"))).scalar()
        sensor_count = (await db.execute(text("SELECT COUNT(*) FROM sensors WHERE is_active = TRUE"))).scalar()
        return {
            "total_containers": container_count,
            "total_anomalies": anomaly_count,
            "active_sensors": sensor_count
        }
    except Exception as e:
        logger.error(f"Error fetching KPIs: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# Serve Interactive Dashboard HTML at Root "/"
static_dir = os.path.join(os.path.dirname(__file__), "static")

@app.get("/")
async def get_dashboard():
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="Dashboard index.html not found")
