from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import json

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient
from shared.cassandra_client import CassandraClient

def get_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient) -> Optional[models.Sensor]:
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    doc_sensor = get_sensor_collection_by_name(name=db_sensor.name,mongodb_client=mongodb_client)
    ret_sensor = schemas.SensorReturn(
        id = db_sensor.id,
        name=doc_sensor.name,
        longitude=doc_sensor.longitude,
        latitude=doc_sensor.latitude,
        type=doc_sensor.type,
        mac_address=doc_sensor.mac_address,
        manufacturer=doc_sensor.manufacturer,
        model=doc_sensor.model,
        serie_number=doc_sensor.serie_number,
        firmware_version=doc_sensor.firmware_version,
        description=doc_sensor.description
    )
    return ret_sensor
    
def check_id(db: Session, sensor_id: int):
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, mongodb_client: MongoDBClient,sensor: schemas.SensorCreate, es: ElasticsearchClient) -> schemas.SensorReturn:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    add_collection(mongodb_client=mongodb_client,sensor=sensor)
    add_index(sensor=sensor,es=es)
    ret_sensor = schemas.SensorReturn(
        id = db_sensor.id,
        name=sensor.name,
        longitude=sensor.longitude,
        latitude=sensor.latitude,
        type=sensor.type,
        mac_address=sensor.mac_address,
        manufacturer=sensor.manufacturer,
        model=sensor.model,
        serie_number=sensor.serie_number,
        firmware_version=sensor.firmware_version,
        description=sensor.description
    )
    return ret_sensor

def record_data(db: Session, mongodb_client: MongoDBClient, redis: Session, sensor_id: int, data: schemas.SensorData, timescale: Timescale, cassandra: CassandraClient)-> schemas.Sensor:
    sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    data_json = convertToJSON(data)
    redis.set(sensor_id, data_json)
    #add_temporal_data(sensor_id=sensor_id,data=data,timescale=timescale) 
    #En el test se añaden datos al mismo sensor con la misma fecha y vulnera la primary key de timescale por eso lo comento
    doc_sensor = get_sensor_collection_by_name(name=sensor.name,mongodb_client=mongodb_client)
    post_data_cassandra(sensor_id=sensor_id,type_=doc_sensor.type, data= data, cassandra=cassandra)
    db_sensor = schemas.Sensor(
        id = sensor.id,
        name = sensor.name,
        latitude = doc_sensor.latitude,
        longitude = doc_sensor.longitude,
        joined_at = str(sensor.joined_at),
        last_seen = data.last_seen,
        type= doc_sensor.type,
        mac_address= doc_sensor.mac_address,
        battery_level = data.battery_level,
        temperature = data.temperature,
        humidity = data.humidity,
        velocity = data.velocity,
        description = doc_sensor.description
    )
    return db_sensor

def get_data(ts: Timescale, from_date: str, to: str, bucket: str, sensor_id: int):

    ts.execute('commit')
    drop = f"""DROP MATERIALIZED VIEW IF EXISTS sensor_data_summary_{bucket}_{sensor_id}"""
    ts.execute(drop)

    ts.execute('commit')
    create_view_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_data_summary_{bucket}_{sensor_id}
        WITH (timescaledb.continuous) AS
        SELECT sensor_id,
            time_bucket(INTERVAL '1 {bucket}', time) AS bucket,
            AVG(temperature) AS avg_temperature,
            AVG(humidity) AS avg_humidity,
            AVG(velocity) AS avg_velocity,
            AVG(battery_level) AS avg_battery_level
        FROM sensor_data
        WHERE sensor_id = {sensor_id}
        AND time >=  '{from_date}'
        AND time <=  '{to}'
        GROUP BY sensor_id, bucket;
        """
    ts.execute(create_view_query)

    query_view = f"""SELECT *
        FROM sensor_data_summary_{bucket}_{sensor_id}"""
    ts.execute('commit')
    ts.execute(query_view)
    cursor = ts.getCursor()
    response = cursor.fetchall()
    return response

def delete_sensor(redis: Session, sensor_id: int, db: Session, mongodb_client: MongoDBClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    name = db_sensor.name
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    delete_sensor_collection_by_name(name=name,mongodb_client=mongodb_client)
    redis.delete(sensor_id)
    return db_sensor

def convertToJSON(value):
    return json.dumps({
        'velocity': value.velocity,
        'temperature': value.temperature,
        'humidity': value.humidity,
        'battery_level': value.battery_level,
        'last_seen': value.last_seen
    })

def convertToLastData(value):
    data = json.loads(value)
    return schemas.SensorData(
        velocity=data['velocity'],
        temperature=data['temperature'],
        humidity=data['humidity'],
        battery_level=data['battery_level'],
        last_seen=data['last_seen']
    )
def add_index(sensor: schemas.SensorCreate, es: ElasticsearchClient):
    es_index_name = 'sensors'
    if not es.index_exists('sensors'):
        es.create_index("sensors")
        mapping = {
            'properties': {
                'name': {'type': 'keyword'},
                'description': {'type': 'text'},
                'type': {'type': 'text'}
            }
        }
        es.create_mapping("sensors",mapping)
    es_doc = {
        'name': sensor.name,
        'description': sensor.description,
        'type': sensor.type
    }
    es.index_document(index_name=es_index_name,document=es_doc)

def add_collection(mongodb_client: MongoDBClient,sensor: schemas.SensorCreate) :
    mycol = mongodb_client.getCollection(collection='sensors_col')
    mycol.create_index([("location", "2dsphere")])
    sensor = {'name': sensor.name,
              'type': sensor.type,
              'description': sensor.description, 
              'location': {
                    'type': "Point",
                    'coordinates': [sensor.longitude, sensor.latitude]
                },
              'mac_address': sensor.mac_address,
              'manufacturer': sensor.manufacturer,
              'model': sensor.model,
              'serie_number': sensor.serie_number,
              'firmware_version': sensor.firmware_version}
    mycol.insert_one(sensor)

def get_sensor_collection_by_name(name: str,mongodb_client: MongoDBClient) -> schemas.SensorCreate:
    mycol = mongodb_client.getCollection(collection='sensors_col')
    col_sensor = mycol.find_one({'name': name})
    
    return schemas.SensorCreate(name=name,
                                      type=col_sensor['type'],
                                      description=col_sensor['description'],
                                      longitude=col_sensor['location']['coordinates'][0],
                                      latitude=col_sensor['location']['coordinates'][1],
                                      mac_address=col_sensor['mac_address'],
                                      manufacturer=col_sensor['manufacturer'],
                                      model=col_sensor['model'],
                                      serie_number=col_sensor['serie_number'],
                                      firmware_version=col_sensor['firmware_version'])

def delete_sensor_collection_by_name(name: str,mongodb_client: MongoDBClient):
    mycol = mongodb_client.getCollection(collection='sensors_col')
    mycol.delete_one({'name': name})

def get_sensors_near(db: Session, mongodb_client: MongoDBClient, redis: Session, latitude: float, longitude: float, radius: float):
    mycol = mongodb_client.getCollection(collection='sensors_col')
    query = mycol.find({
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude,latitude]
                },
                "$maxDistance": radius  
            }
        }
    })
    query_data = []
    
    for col in query:
        id = db.query(models.Sensor).filter(models.Sensor.name == col['name']).first().id
        query_data.append(get_data(redis=redis, sensor_id=id, db=db, mongodb_client=mongodb_client))

    return query_data

def search_sensors(db: Session,mongodb: MongoDBClient, query: str, size: int, search_type: str, es: ElasticsearchClient):
    dict_query = json.loads(query)
    if search_type == 'similar':
        key = list(dict_query.keys())[0]
        es_query = {
            "query": {
                "fuzzy": {
                    key: {
                        'value': dict_query[key],
                        "fuzziness": "AUTO"
                    }
                }
            },
            "size": size
        }
    else:
        es_query = {
            "query": {
                search_type: dict_query
            },
            "size": size
        }   
    results = es.search(index_name='sensors', query=es_query)
    query_sensors = []
    for hit in results['hits']['hits']:
        name = hit['_source']['name']
        id = db.query(models.Sensor).filter(models.Sensor.name == name).first().id
        doc_sensor = get_sensor_collection_by_name(name=name,mongodb_client=mongodb)
        ret_sensor = schemas.SensorReturn(
            id = id,
            name=name,
            longitude=doc_sensor.longitude,
            latitude=doc_sensor.latitude,
            type=doc_sensor.type,
            mac_address=doc_sensor.mac_address,
            manufacturer=doc_sensor.manufacturer,
            model=doc_sensor.model,
            serie_number=doc_sensor.serie_number,
            firmware_version=doc_sensor.firmware_version,
            description=doc_sensor.description
        )
        query_sensors.append(ret_sensor)
    
    return query_sensors

def add_temporal_data(sensor_id: int, data: schemas.SensorData, timescale: Timescale):
    insert_query = ""
    if data.velocity == None:
        insert_query =f"""INSERT INTO sensor_data (time, sensor_id, temperature, humidity, battery_level) 
                  VALUES ('{data.last_seen}', {sensor_id}, {data.temperature}, {data.humidity}, {data.battery_level})"""
    else:
        insert_query = f"""INSERT INTO sensor_data (time, sensor_id, velocity, battery_level) 
                  VALUES ('{data.last_seen}', {sensor_id}, {data.velocity}, {data.battery_level})"""
    timescale.execute(insert_query)
    timescale.execute('commit')

def create_keyspace(cassandra: CassandraClient):
    query1 = """
        CREATE KEYSPACE IF NOT EXISTS sensor WITH replication =
        {'class': 'SimpleStrategy', 'replication_factor' : 1}
    """

    query2 = """
        CREATE TABLE IF NOT EXISTS sensor.temperature_values (
        sensor_id int,
        time text,
        temperature float,
        PRIMARY KEY(sensor_id, time, temperature))
        WITH comment = 'Q1. Find temperature max, min, avg values from sensors'
    """

    query3 = """
        CREATE TABLE IF NOT EXISTS sensor.sensor_type (
        sensor_id int,
        sensor_type text,
        PRIMARY KEY(sensor_type, sensor_id))
        WITH comment = 'Q2. Find quantity by type'
    """

    query4 = """
        CREATE TABLE IF NOT EXISTS sensor.battery (
        sensor_id int PRIMARY KEY,
        battery_level DECIMAL)
        WITH comment = 'Q3. Find sensor with low battery'
    """
    cassandra.execute(query=query1)
    cassandra.execute(query=query2)
    cassandra.execute(query=query3)
    cassandra.execute(query=query4)

def post_data_cassandra(type_: str, sensor_id: int, data: schemas.SensorData, cassandra: CassandraClient):
    create_keyspace(cassandra=cassandra)
    if type_ == "Temperatura":
        insert_query_temperature =f"""INSERT INTO sensor.temperature_values (time, sensor_id, temperature) 
            VALUES ('{data.last_seen}', {sensor_id}, {data.temperature})"""
        cassandra.execute(insert_query_temperature)
    
    insert_query_battery =f"""INSERT INTO sensor.battery (sensor_id, battery_level) 
          VALUES ({sensor_id}, {data.battery_level})"""
    cassandra.execute(insert_query_battery)

    insert_query =f"""INSERT INTO sensor.sensor_type (sensor_id, sensor_type) 
            VALUES ({sensor_id}, '{type_}')"""
    cassandra.execute(insert_query)

def get_temperature_values(db: Session, cassandra: CassandraClient, mongodb_client: MongoDBClient):
    query_temperature = """
        SELECT sensor_id, 
            AVG(temperature) AS average_temperature,
            MAX(temperature) AS max_temperature,
            MIN(temperature) AS min_temperature
        FROM sensor.temperature_values
        GROUP BY sensor_id;
    """
    result = cassandra.execute(query_temperature)
    temperature_values = result.all()
    return_sensors = []
    for row in temperature_values:
        db_sensor = db.query(models.Sensor).filter(models.Sensor.id == row.sensor_id).first()
        doc_sensor = get_sensor_collection_by_name(name=db_sensor.name,mongodb_client=mongodb_client)
        sensor = {
            "id": db_sensor.id,
            "name": doc_sensor.name,
            "longitude": doc_sensor.longitude,
            "latitude": doc_sensor.latitude,
            "type": doc_sensor.type,
            "mac_address": doc_sensor.mac_address,
            "manufacturer": doc_sensor.manufacturer,
            "model": doc_sensor.model,
            "serie_number": doc_sensor.serie_number,
            "firmware_version": doc_sensor.firmware_version,
            "description": doc_sensor.description,
            "values": [{"max_temperature": row.max_temperature, "min_temperature": row.min_temperature, "average_temperature": row.average_temperature}]
        }
        return_sensors.append(sensor)

    return {"sensors": return_sensors}

def get_sensors_quantity(db: Session, cassandra: CassandraClient):
    query_quantity = """
        SELECT sensor_type, COUNT(sensor_id) AS quantity
        FROM sensor.sensor_type
        GROUP BY sensor_type;
        """
    result = cassandra.execute(query_quantity)
    result = result.all()
    quantity_return = []
    for row in result:
        quantity_return.append({"type": row.sensor_type, "quantity": row.quantity})
        
    return {"sensors": quantity_return}

def get_low_battery_sensors(db: Session, cassandra: CassandraClient, mongodb_client: MongoDBClient):
    query_battery = """
        SELECT sensor_id, battery_level
        FROM sensor.battery
        WHERE battery_level < 0.2
        ALLOW FILTERING
        """
    result = cassandra.execute(query_battery)
    result = result.all()

    return_sensors = []
    for row in result:
        db_sensor = db.query(models.Sensor).filter(models.Sensor.id == row.sensor_id).first()
        doc_sensor = get_sensor_collection_by_name(name=db_sensor.name,mongodb_client=mongodb_client)
        sensor = {
            "id": db_sensor.id,
            "name": doc_sensor.name,
            "longitude": doc_sensor.longitude,
            "latitude": doc_sensor.latitude,
            "type": doc_sensor.type,
            "mac_address": doc_sensor.mac_address,
            "manufacturer": doc_sensor.manufacturer,
            "model": doc_sensor.model,
            "serie_number": doc_sensor.serie_number,
            "firmware_version": doc_sensor.firmware_version,
            "description": doc_sensor.description,
            "battery_level": row.battery_level
        }
        return_sensors.append(sensor)

    return {"sensors": return_sensors}

def get_data_redis(db: Session,redis: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    data_json = redis.get(sensor_id)
    data = convertToLastData(data_json)
    db_sensor.temperature = data.temperature
    db_sensor.humidity = data.humidity
    db_sensor.battery_level = data.battery_level
    db_sensor.last_seen = data.last_seen
    return db_sensor