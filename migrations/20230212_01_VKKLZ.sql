-- 
-- depends: 

CREATE TABLE IF NOT EXISTS sensors ( id integer PRIMARY KEY, name varchar(255) NOT NULL UNIQUE, latitude float NOT NULL, longitude float NOT NULL, joined_at timestamp NOT NULL, last_seen timestamp NOT NULL, temperature float NOT NULL, humidity float NOT NULL, battery_level float NOT NULL );