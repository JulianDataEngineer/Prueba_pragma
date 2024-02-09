-- create external table 
CREATE EXTERNAL TABLE IF NOT EXISTS pragma.dos( timestamp timestamp, price INT, user_id INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '\"') STORED AS TEXTFILE LOCATION '/user/hdfs/dos' tblproperties ('skip.header.line.count'='1','external.table.purge'='false') -- create ORC table 
DROP TABLE IF EXISTS pragma.dos_transactional;

 CREATE TABLE pragma.dos_transactional( timestamp timestamp, price INT, user_id INT ) STORED AS ORC TBLPROPERTIES ('transactional'='true');

 -- Insert data, external to ORC 
 INSERT OVERWRITE TABLE pragma.dos_transactional SELECT * FROM pragma.dos;

 INSERT OVERWRITE TABLE pragma.dos_transactional SELECT timestamp,
         price,
         user_id FROM pragma.dos -- validation of calculations 
SELECT *
FROM pragma.dos;

SELECT *
FROM pragma.dos_transactional;

 -- Recuento de filas cargadas a la base de datos 
SELECT COUNT(*) AS total_filas FROM pragma.dos_transactional;

 -- Valor medio,
         mínimo y máximo del campo "price" 
SELECT AVG(price) AS valor_medio,
         MIN(price) AS valor_minimo,
         MAX(price) AS valor_maximo
FROM pragma.dos_transactional;

 -- Drop tables 
DROP TABLE pragma.dos DROP TABLE pragma.dos_transactional 