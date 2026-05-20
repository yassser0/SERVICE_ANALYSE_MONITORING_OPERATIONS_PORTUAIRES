from sqlalchemy import Column, String, Numeric, Boolean, DateTime, text, func, MetaData
from sqlalchemy.dialects.postgresql import UUID, JSONB, ENUM
from database import Base

metadata = MetaData()

class Sensor(Base):
    __tablename__ = "sensors"
    
    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    sensor_id = Column(String(50), unique=True, nullable=False)
    sensor_type = Column(String, nullable=False)
    zone = Column(String)
    location_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    metadata_ = Column("metadata", JSONB)
    last_seen = Column(DateTime(timezone=True))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class Container(Base):
    __tablename__ = "containers"
    
    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    container_id = Column(String(20), unique=True, nullable=False)
    current_zone = Column(String)
    current_lat = Column(Numeric(10, 7))
    current_lon = Column(Numeric(10, 7))
    equipment_status = Column(String, default='ACTIVE')
    temperature_celsius = Column(Numeric(5, 2))
    last_seen = Column(DateTime(timezone=True))
    is_flagged = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

# For sensor_events (partitioned), we often insert via raw SQL or handle carefully
class SensorEvent(Base):
    __tablename__ = "sensor_events"
    
    # We map this for querying, but insertion into partitioned tables
    # usually needs to just hit the parent table.
    # Note: id + event_timestamp is primary key in postgres schema
    id = Column(Numeric, primary_key=True) 
    event_timestamp = Column(DateTime(timezone=True), primary_key=True)
    event_id = Column(UUID(as_uuid=True))
    sensor_id = Column(String(50), nullable=False)
    container_id = Column(String(20))
    temperature_celsius = Column(Numeric(5, 2))
    zone = Column(String)
    equipment_status = Column(String)
    latitude = Column(Numeric(10, 7))
    longitude = Column(Numeric(10, 7))
    raw_payload = Column(JSONB, nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())
    is_anomaly = Column(Boolean, default=False)

class Anomaly(Base):
    __tablename__ = "anomalies"
    
    id = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    event_id = Column(UUID(as_uuid=True))
    sensor_id = Column(String(50), nullable=False)
    container_id = Column(String(20))
    anomaly_type = Column(ENUM('HIGH_TEMPERATURE', 'INACTIVE_CONTAINER', 'INVALID_GPS', 'EQUIPMENT_ERROR', 'SENSOR_OFFLINE', 'UNEXPECTED_MOVEMENT', name='anomaly_type_enum', create_type=False), nullable=False)
    severity = Column(ENUM('LOW', 'MEDIUM', 'HIGH', 'CRITICAL', name='severity_enum', create_type=False), default='MEDIUM')
    description = Column(String, nullable=False)
    detected_value = Column(Numeric(10, 4))
    threshold_value = Column(Numeric(10, 4))
    zone = Column(ENUM('QUAY_A', 'QUAY_B', 'QUAY_C', 'YARD_NORTH', 'YARD_SOUTH', 'GATE_ENTRY', 'GATE_EXIT', 'WAREHOUSE_1', 'WAREHOUSE_2', 'MAINTENANCE_AREA', name='zone_enum', create_type=False))
    is_resolved = Column(Boolean, default=False)
    raw_payload = Column(JSONB)
    detected_at = Column(DateTime(timezone=True), server_default=func.now())
