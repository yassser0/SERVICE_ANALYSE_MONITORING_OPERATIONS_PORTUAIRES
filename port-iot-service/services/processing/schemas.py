from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime
from uuid import UUID


class SensorDataPayload(BaseModel):
    event_id: str
    sensor_id: str
    timestamp: datetime
    zone: Optional[str] = None
    container_id: Optional[str] = None
    vessel_id: Optional[str] = None
    # GPS / Movement
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    speed_kmh: Optional[float] = None
    heading_degrees: Optional[float] = None
    # Temperature / Environment
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    # Equipment
    equipment_status: Optional[str] = None
    load_percentage: Optional[float] = None
    operating_hours: Optional[float] = None
    # Sensor health
    battery_level: Optional[float] = None     # 0–100 %
    signal_strength: Optional[float] = None   # dBm (e.g. -70)


class SensorResponse(BaseModel):
    sensor_id: str
    sensor_type: str
    zone: Optional[str] = None
    is_active: bool
    last_seen: Optional[datetime] = None

    class Config:
        from_attributes = True


class ContainerResponse(BaseModel):
    container_id: str
    vessel_id: Optional[str] = None
    current_zone: Optional[str] = None
    current_lat: Optional[float] = None
    current_lon: Optional[float] = None
    equipment_status: Optional[str] = None
    temperature_celsius: Optional[float] = None
    is_flagged: bool
    last_seen: Optional[datetime] = None

    class Config:
        from_attributes = True


class AnomalyResponse(BaseModel):
    id: UUID
    sensor_id: str
    container_id: Optional[str] = None
    anomaly_type: str
    severity: str
    description: str
    detected_at: datetime
    is_resolved: bool
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class VesselResponse(BaseModel):
    vessel_id: str
    vessel_name: str
    flag_country: Optional[str] = None
    vessel_type: Optional[str] = None
    berth_location: Optional[str] = None
    status: str
    arrival_time: Optional[datetime] = None
    departure_time: Optional[datetime] = None
    captain_name: Optional[str] = None
    gross_tonnage: Optional[float] = None

    class Config:
        from_attributes = True


class ResolveAnomalyRequest(BaseModel):
    resolved_by: str = Field(..., min_length=1, max_length=100)
    notes: Optional[str] = None
