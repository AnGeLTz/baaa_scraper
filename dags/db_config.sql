-- Create the raw_data schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS raw_data;


-- Switch to the raw_data schema
SET search_path TO raw_data;


-- Drop the table if it already exists
DROP TABLE IF EXISTS accidents;


-- Create the table accidents
CREATE TABLE raw_data.accidents (
    id SERIAL PRIMARY KEY,
    date VARCHAR(255),
    type VARCHAR(255),
    operator VARCHAR(255),
    flight_phase VARCHAR(255),
    flight_type VARCHAR(255),
    survivors VARCHAR(255),
    site VARCHAR(255),
    schedule VARCHAR(255),
    msn VARCHAR(255),
    yom INTEGER,
    flight_number VARCHAR(255),
    city VARCHAR(255),
    zone VARCHAR(255),
    country VARCHAR(255),
    region VARCHAR(255),
    crew_on_board INTEGER,
    crew_fatalities INTEGER,
    pax_on_board INTEGER,
    pax_fatalities INTEGER,
    other_fatalities INTEGER,
    total_fatalities INTEGER,
    captain_hours INTEGER,
    captain_hours_type INTEGER,
    copilot_hours INTEGER,
    copilot_hours_type INTEGER,
    aircraft_hours INTEGER,
    circumstances TEXT,
    probable_cause TEXT
);

CREATE SCHEMA IF NOT EXISTS statistics;


-- Switch to the statistics schema
SET search_path TO statistics;


-- Drop the table if it already exists
DROP TABLE IF EXISTS air_accidents;


-- Create the table air_accidents
CREATE TABLE statistics.air_accidents (
    id SERIAL PRIMARY KEY,
    date DATE,
    type VARCHAR(255),
    operator VARCHAR(255),
    flight_phase VARCHAR(255),
    flight_type VARCHAR(255),
    survivors VARCHAR(255),
    site VARCHAR(255),
    schedule VARCHAR(255),
    msn VARCHAR(255),
    yom INTEGER,
    flight_number VARCHAR(255),
    city VARCHAR(255),
    zone VARCHAR(255),
    country VARCHAR(255),
    region VARCHAR(255),
    crew_on_board INTEGER,
    crew_fatalities INTEGER,
    pax_on_board INTEGER,
    pax_fatalities INTEGER,
    other_fatalities INTEGER,
    total_fatalities INTEGER,
    captain_hours INTEGER,
    captain_hours_type INTEGER,
    copilot_hours INTEGER,
    copilot_hours_type INTEGER,
    aircraft_hours INTEGER,
    circumstances TEXT,
    probable_cause TEXT
);

CREATE TABLE IF NOT EXISTS statistics.scheduled_revenue_flight (
  id SERIAL PRIMARY KEY,
  year VARCHAR(4),
  flights FLOAT
);

INSERT INTO statistics.scheduled_revenue_flight (year, flights) VALUES
  ('2004', 23.8),
  ('2005', 24.9),
  ('2006', 25.5),
  ('2007', 26.7),
  ('2008', 26.5),
  ('2009', 25.9),
  ('2010', 27.8),
  ('2011', 30.1),
  ('2012', 31.2),
  ('2013', 32.0),
  ('2014', 33.0),
  ('2015', 34.0),
  ('2016', 35.2),
  ('2017', 36.4),
  ('2018', 38.1),
  ('2019', 38.9),
  ('2020', 56.7),
  ('2021', 22.2);