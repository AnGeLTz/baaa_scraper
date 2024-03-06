-- format amendment
UPDATE raw_data.accidents 
SET date = TO_DATE(date, 'YYYY-MM-DD');

-- moves data from raw_data table to statistics table
INSERT INTO statistics.air_accidents 
SELECT *  FROM raw_data.accidents;

-- cleaning
UPDATE statistics.air_accidents 
SET operator = REPLACE(operator, '%20', ' ');

UPDATE statistics.air_accidents
SET operator = REPLACE(operator, '%26', ' ');

UPDATE statistics.air_accidents 
SET operator = REPLACE(operator, '%27', ' ');


UPDATE statistics.air_accidents 
SET operator = REPLACE(operator, '.png', '');

UPDATE  statistics.air_accidents 
SET operator = REPLACE(operator, '.gif', '');

