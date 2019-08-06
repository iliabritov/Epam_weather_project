CREATE USER docker;

CREATE DATABASE dbweather;

GRANT ALL PRIVILEGES ON DATABASE dbweather TO docker;

\c dbweather;

DROP TABLE IF EXISTS weather; 
CREATE TABLE weather (
	id SERIAL PRIMARY KEY,
	data_base VARCHAR(20),
	city VARCHAR(20),
	date_day DATE,
	temp REAL,
	prec_desc VARCHAR(500),
	prec_mm REAL, 
	wind_speed REAL,
	wind_direc VARCHAR(100)
	);
