from fastapi.testclient import TestClient
import pytest
from app.main import app
from app.redis_client import RedisClient
from app.mongodb_client import MongoDBClient
from app.elasticsearch_client import ElasticsearchClient
from app.timescale import Timescale
from app.cassandra_client import CassandraClient

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
     from app.database import SessionLocal, engine
     from app.sensors import models
     models.Base.metadata.drop_all(bind=engine)
     models.Base.metadata.create_all(bind=engine)
     redis = RedisClient(host="redis")
     redis.clearAll()
     redis.close()
     mongo = MongoDBClient(host="mongodb")
     mongo.clearDb("sensors")
     mongo.close()

     cassandra = CassandraClient(["cassandra"])
     cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
     cassandra.close()

     


#TODO ADD all your tests in test_*.py files: