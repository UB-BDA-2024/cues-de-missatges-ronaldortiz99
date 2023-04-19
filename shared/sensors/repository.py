from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(db: Session, mongodb_clinet: MongoDBClient, sensor: schemas.SensorCreate) -> models.Sensor:
    # Add sensor to postgres database
    db_sensor = add_sensor_to_postgres(db, sensor)

    # Add sensor to mongodb database
    mongo_sensor = add_sensor_to_mongodb(mongodb_clinet, sensor, db_sensor.id)

    del mongo_sensor['location']
    mongo_sensor['latitude'] = sensor.latitude
    mongo_sensor['longitude'] = sensor.longitude

    return mongo_sensor


def add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    date = datetime.now()

    db_sensor = models.Sensor(name=sensor.name, joined_at=date)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    return db_sensor


def add_sensor_to_mongodb(mongodb_client: MongoDBClient, db_sensor: schemas.SensorCreate, id):
    mongo_projection = schemas.SensorMongoProjection(id=id, name=db_sensor.name, location={'type': 'Point',
                                                                                           'coordinates': [
                                                                                               db_sensor.longitude,
                                                                                               db_sensor.latitude]},
                                                     type=db_sensor.type, mac_address=db_sensor.mac_address,
                                                     description=db_sensor.description,
                                                     serie_number=db_sensor.serie_number,
                                                     firmware_version=db_sensor.firmware_version, model=db_sensor.model,
                                                     manufacturer=db_sensor.manufacturer)
    mongodb_client.getDatabase()
    mongoInsert = mongo_projection.dict()
    mongodb_client.getCollection().insert_one(mongoInsert)
    return mongo_projection.dict()


def record_data(redis: RedisClient, timescale: Timescale, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    # We store the recieved data as a JSON string in Redis
    redis.set(sensor_id, data.json())

    # We set the data to the timescale database
    data_to_insert = data.dict()
    data_to_insert['sensor_id'] = sensor_id
    data_to_insert['time'] = data_to_insert['last_seen']
    del data_to_insert['last_seen']

    query = timescale.generate_insert_query('sensor_data', data_to_insert)
    print(query)
    var = timescale.execute(query)
    print(var)

    return data


def getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")


def get_data(timescale: Timescale, sensor_id: int, dataCommand: DataCommand) -> schemas.Sensor:
    # We need to get the bucket to know wich view query on timescale
    view = getView(dataCommand.bucket)
    query = f"SELECT * FROM {view} WHERE sensor_id = {sensor_id} AND bucket >= '{dataCommand.from_time}' AND bucket <= '{dataCommand.to_time}'"
    data = timescale.execute(query, True)
    return data


def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor