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
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    temperature: Optional[float] = None
    equipment_status: Optional[str] = None

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
    current_zone: Optional[str] = None
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

    class Config:
        from_attributes = True
