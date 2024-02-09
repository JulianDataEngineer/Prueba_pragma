-- create external table 
CREATE EXTERNAL TABLE IF NOT EXISTS pragma.tres( timestamp timestamp, price INT, user_id INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '\"') STORED AS TEXTFILE LOCATION '/user/hdfs/tres' tblproperties ('skip.header.line.count'='1','external.table.purge'='false') -- create ORC table 
DROP TABLE IF EXISTS pragma.tres_transactional;

 CREATE TABLE pragma.tres_transactional( timestamp timestamp, price INT, user_id INT ) STORED AS ORC TBLPROPERTIES ('transactional'='true');

 -- Insert data, external to ORC 
 INSERT OVERWRITE TABLE pragma.tres_transactional SELECT * FROM pragma.tres;

 INSERT OVERWRITE TABLE pragma.tres_transactional SELECT timestamp,
         price,
         user_id FROM pragma.tres -- validation of calculations 
SELECT *
FROM pragma.tres;

SELECT *
FROM pragma.tres_transactional;

 -- Recuento de filas cargadas a la base de datos 
SELECT COUNT(*) AS total_filas FROM pragma.tres_transactional;

 -- Valor medio,
         mínimo y máximo del campo "price" 
SELECT AVG(price) AS valor_medio,
         MIN(price) AS valor_minimo,
         MAX(price) AS valor_maximo
FROM pragma.tres_transactional;

 -- Drop tables 
DROP TABLE pragma.tres DROP TABLE pragma.tres_transactional