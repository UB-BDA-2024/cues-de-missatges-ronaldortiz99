import json

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from shared.database import SessionLocal
from shared.publisher import Publisher
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient
from shared.timescale import Timescale
from shared.sensors import repository, schemas


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_timescale():
    ts = Timescale()
    try:
        yield ts
    finally:
        ts.close()

# Dependency to get redis client

def get_redis_client():
    redis = RedisClient(host="redis")
    try:
        yield redis
    finally:
        redis.close()

# Dependency to get mongodb client

def get_mongodb_client():
    mongodb = MongoDBClient(host="mongodb")   
    mongodb.getDatabase(database="sensors")
    try:
        yield mongodb
    finally:
        mongodb.close()

# Dependency to get elastic_search client
def get_elastic_search():
    es = ElasticsearchClient(host="elasticsearch")
    try:
        yield es
    finally:
        es.close()

# Dependency to get cassandra client
def get_cassandra_client():
    cassandra = CassandraClient(hosts=["cassandra"])
    try:
        yield cassandra
    finally:
        cassandra.close()

publisher = Publisher()

router = APIRouter(
    prefix="/sensors",
    responses={404: {"description": "Not found"}},
    tags=["sensors"],
)


# 🙋🏽‍♀️ Add here the route to get a list of sensors near to a given location
@router.get("/near")
def get_sensors_near(latitude: float, longitude: float, radius: float, db: Session = Depends(get_db) , mongodb_client: MongoDBClient = Depends(get_mongodb_client), redis_client: RedisClient = Depends(get_redis_client)):
    return repository.get_sensors_near(db=db,redis=redis_client,radius=radius,mongodb_client=mongodb_client, latitude=latitude, longitude=longitude)


# 🙋🏽‍♀️ Add here the route to search sensors by query to Elasticsearch
# Parameters:
# - query: string to search
# - size (optional): number of results to return
# - search_type (optional): type of search to perform
# - db: database session
# - mongodb_client: mongodb client
@router.get("/search")
def search_sensors(query: str, size: int = 10, search_type: str = "match", db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), es: ElasticsearchClient = Depends(get_elastic_search)):
    return repository.search_sensors(db=db,mongodb=mongodb_client, query=query, size=size, search_type=search_type, es=es)

# 🙋🏽‍♀️ Add here the route to get the temperature values of a sensor

@router.get("/temperature/values")
def get_temperature_values(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    return repository.get_temperature_values(db=db, cassandra=cassandra_client, mongodb_client=mongodb_client)

@router.get("/quantity_by_type")
def get_sensors_quantity(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    return repository.get_sensors_quantity(db=db, cassandra=cassandra_client)

@router.get("/low_battery")
def get_low_battery_sensors(db: Session = Depends(get_db), cassandra_client: CassandraClient = Depends(get_cassandra_client), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    return repository.get_low_battery_sensors(db=db, cassandra=cassandra_client, mongodb_client=mongodb_client)

# 🙋🏽‍♀️ Add here the route to get all sensors
@router.get("")
def get_sensors(db: Session = Depends(get_db)):
    return repository.get_sensors(db)


# 🙋🏽‍♀️ Add here the route to create a sensor
@router.post("")
def create_sensor(sensor: schemas.SensorCreate, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client), es: ElasticsearchClient = Depends(get_elastic_search)):
    db_sensor = repository.get_sensor_by_name(db, sensor.name)
    if db_sensor:
        raise HTTPException(status_code=400, detail="Sensor with same name already registered")
    return repository.create_sensor(db=db, mongodb_client=mongodb_client,sensor=sensor, es=es)

# 🙋🏽‍♀️ Add here the route to get a sensor by id
@router.get("/{sensor_id}")
def get_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client)):
    db_sensor = repository.check_id(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.get_sensor(sensor_id=sensor_id,db=db,mongodb_client=mongodb_client)

# 🙋🏽‍♀️ Add here the route to delete a sensor
@router.delete("/{sensor_id}")
def delete_sensor(sensor_id: int, db: Session = Depends(get_db), mongodb_client: MongoDBClient = Depends(get_mongodb_client),redis_client: RedisClient = Depends(get_redis_client)):
    db_sensor = repository.check_id(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.delete_sensor(db=db, sensor_id=sensor_id, redis=redis_client, mongodb_client=mongodb_client)
    

# 🙋🏽‍♀️ Add here the route to update a sensor
@router.post("/{sensor_id}/data")
def record_data(sensor_id: int, data: schemas.SensorData,db: Session = Depends(get_db) , mongodb_client: MongoDBClient = Depends(get_mongodb_client),redis_client: RedisClient = Depends(get_redis_client),  timescale: Timescale = Depends(get_timescale),  cassandra_client: CassandraClient = Depends(get_cassandra_client)):
    db_sensor = repository.check_id(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    return repository.record_data(db=db,redis=redis_client, mongodb_client=mongodb_client, sensor_id=sensor_id, data=data, timescale=timescale, cassandra=cassandra_client)

# 🙋🏽‍♀️ Add here the route to get data from a sensor
@router.get("/{sensor_id}/data")
def get_data(sensor_id: int, request: Request, to: str = None, bucket: str = None, db: Session = Depends(get_db) , mongodb_client: MongoDBClient = Depends(get_mongodb_client), redis_client: RedisClient = Depends(get_redis_client),  timescale: Timescale = Depends(get_timescale)):  
    db_sensor = repository.check_id(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")  
    if bucket != None:
        full_url = str(request.url)
        from_date = full_url.split('?')[1].split('&')[0].split('=')[1]
        return repository.get_data(ts=timescale,from_date=from_date,to=to,bucket=bucket,sensor_id=sensor_id)
    else:
        return repository.get_data_redis(db=db,sensor_id=sensor_id)



class ExamplePayload():
    def __init__(self, example):
        self.example = example

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
@router.post("/exemple/queue")
def exemple_queue():
    # Publish here the data to the queue
    publisher.publish(ExamplePayload("holaaaaa"))
    return {"message": "Data published to the queue"}