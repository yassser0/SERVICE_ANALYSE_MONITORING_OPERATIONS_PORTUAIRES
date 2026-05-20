from sqlalchemy import Column, String, Numeric, Boolean, DateTime, text, func, MetaData
from sqlalchemy.dialects.postgresql import UUID, JSONB, ENUM
from database import Base

metadata = MetaData()


class Sensor(Base):
    __tablename__ = "sensors"

    id               = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    sensor_id        = Column(String(50), unique=True, nullable=False)
    sensor_type      = Column(String, nullable=False)
    zone             = Column(String)
    location_name    = Column(String(100))
    manufacturer     = Column(String(100))
    model            = Column(String(100))
    firmware_version = Column(String(20))
    is_active        = Column(Boolean, default=True)
    metadata_        = Column("metadata", JSONB)
    last_seen        = Column(DateTime(timezone=True))
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Vessel(Base):
    __tablename__ = "vessels"

    vessel_id        = Column(String(20), primary_key=True)
    vessel_name      = Column(String(100), nullable=False)
    flag_country     = Column(String(50))
    vessel_type      = Column(String(50))
    imo_number       = Column(String(20))
    berth_location   = Column(String)
    arrival_time     = Column(DateTime(timezone=True))
    departure_time   = Column(DateTime(timezone=True))
    status           = Column(String, default='DOCKED')
    captain_name     = Column(String(100))
    gross_tonnage    = Column(Numeric(12, 2))
    created_at       = Column(DateTime(timezone=True), server_default=func.now())
    updated_at       = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class Container(Base):
    __tablename__ = "containers"

    id                  = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    container_id        = Column(String(20), unique=True, nullable=False)
    owner               = Column(String(100))
    container_type      = Column(String(20))
    vessel_id           = Column(String(20))
    current_zone        = Column(String)
    current_lat         = Column(Numeric(10, 7))
    current_lon         = Column(Numeric(10, 7))
    prev_lat            = Column(Numeric(10, 7))
    prev_lon            = Column(Numeric(10, 7))
    equipment_status    = Column(String, default='ACTIVE')
    temperature_celsius = Column(Numeric(5, 2))
    last_movement       = Column(DateTime(timezone=True))
    last_seen           = Column(DateTime(timezone=True))
    is_flagged          = Column(Boolean, default=False)
    created_at          = Column(DateTime(timezone=True), server_default=func.now())
    updated_at          = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class SensorEvent(Base):
    __tablename__ = "sensor_events"

    id                  = Column(Numeric, primary_key=True)
    event_timestamp     = Column(DateTime(timezone=True), primary_key=True)
    event_id            = Column(UUID(as_uuid=True))
    sensor_id           = Column(String(50), nullable=False)
    container_id        = Column(String(20))
    vessel_id           = Column(String(20))
    temperature_celsius = Column(Numeric(5, 2))
    humidity            = Column(Numeric(5, 2))
    zone                = Column(String)
    equipment_status    = Column(String)
    latitude            = Column(Numeric(10, 7))
    longitude           = Column(Numeric(10, 7))
    speed_kmh           = Column(Numeric(7, 2))
    heading_degrees     = Column(Numeric(6, 2))
    battery_level       = Column(Numeric(5, 2))
    signal_strength     = Column(Numeric(7, 2))
    load_percentage     = Column(Numeric(5, 2))
    raw_payload         = Column(JSONB, nullable=False)
    processed_at        = Column(DateTime(timezone=True), server_default=func.now())
    is_anomaly          = Column(Boolean, default=False)


class Anomaly(Base):
    __tablename__ = "anomalies"

    id              = Column(UUID(as_uuid=True), primary_key=True, server_default=text("uuid_generate_v4()"))
    event_id        = Column(UUID(as_uuid=True))
    sensor_id       = Column(String(50), nullable=False)
    container_id    = Column(String(20))
    anomaly_type    = Column(ENUM(
        'HIGH_TEMPERATURE', 'INACTIVE_CONTAINER', 'INVALID_GPS',
        'EQUIPMENT_ERROR', 'SENSOR_OFFLINE', 'UNEXPECTED_MOVEMENT',
        'LOW_BATTERY', 'SPEED_VIOLATION',
        name='anomaly_type_enum', create_type=False
    ), nullable=False)
    severity        = Column(ENUM('LOW', 'MEDIUM', 'HIGH', 'CRITICAL',
                                  name='severity_enum', create_type=False), default='MEDIUM')
    description     = Column(String, nullable=False)
    detected_value  = Column(Numeric(10, 4))
    threshold_value = Column(Numeric(10, 4))
    zone            = Column(ENUM(
        'QUAY_A', 'QUAY_B', 'QUAY_C', 'YARD_NORTH', 'YARD_SOUTH',
        'GATE_ENTRY', 'GATE_EXIT', 'WAREHOUSE_1', 'WAREHOUSE_2', 'MAINTENANCE_AREA',
        name='zone_enum', create_type=False
    ))
    is_resolved     = Column(Boolean, default=False)
    resolved_at     = Column(DateTime(timezone=True))
    resolved_by     = Column(String(100))
    raw_payload     = Column(JSONB)
    detected_at     = Column(DateTime(timezone=True), server_default=func.now())
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
