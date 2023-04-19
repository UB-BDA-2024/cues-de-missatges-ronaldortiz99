from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from app.subscriber import Subscriber
from . import models, schemas

#subscriber = Subscriber()


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data
    return db_sensordata

def get_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data
    return db_sensordata

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor