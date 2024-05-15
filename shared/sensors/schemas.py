from pydantic import BaseModel
from typing import Optional

class Sensor(BaseModel):
    id: int
    name: str
    latitude: float
    longitude: float
    joined_at: str
    last_seen: str
    type: str
    mac_address: str
    battery_level: float
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    velocity: Optional[float] = None
    description: str
    
    
    class Config:
        orm_mode = True
        
class SensorCreate(BaseModel):
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    description: str
    firmware_version: str

class SensorReturn(BaseModel):
    id: int
    name: str
    longitude: float
    latitude: float
    type: str
    mac_address: str
    manufacturer: str
    model: str
    serie_number: str
    firmware_version: str
    description: str

class SensorData(BaseModel):
    velocity: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None
    battery_level: float
    last_seen: str