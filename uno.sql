-- Create DataBase 
CREATE DATABASE pragma;

 -- create external table 
CREATE EXTERNAL TABLE IF NOT EXISTS pragma.uno( timestamp timestamp, price INT, user_id INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '\"') STORED AS TEXTFILE LOCATION '/user/hdfs/primero' tblproperties ('skip.header.line.count'='1','external.table.purge'='false') -- create ORC table 
DROP TABLE IF EXISTS pragma.uno_transactional;

 CREATE TABLE pragma.uno_transactional( timestamp timestamp, price INT, user_id INT ) STORED AS ORC TBLPROPERTIES ('transactional'='true');

 -- Insert data, external to ORC 
 INSERT OVERWRITE TABLE pragma.uno_transactionalSELECT *
FROM default.uno;

 INSERT OVERWRITE TABLE pragma.uno_transactionalSELECT timestamp,
         price,
         user_id
FROM pragma.uno -- validation of calculations 
 SELECT * FROM pragma.uno;

 SELECT * FROM pragma.uno_transactional;

 -- Recuento de filas cargadas a la base de datos 
SELECT COUNT(*) AS total_filas
FROM pragma.uno_transactional;

 -- Valor medio, mínimo y máximo del campo "price" 
SELECT AVG(price) AS valor_medio,
         MIN(price) AS valor_minimo,
         MAX(price) AS valor_maximo FROM pragma.uno_transactional;

 -- Drop tables 
DROP TABLE default.uno;

 DROP TABLE default.uno_transactional;

 