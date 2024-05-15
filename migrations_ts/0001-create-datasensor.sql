-- #TODO: Create new TS hypertable
-- 
-- depends: 

CREATE TABLE IF NOT EXISTS sensor_data (
   time TIMESTAMPTZ NOT NULL,
   sensor_id   integer NOT NULL,
   velocity float   NULL,
   temperature float    NULL,
   humidity    float    NULL,
   battery_level float NOT NULL,
   PRIMARY KEY (time, sensor_id)
);

SELECT create_hypertable('sensor_data', by_range('time'));