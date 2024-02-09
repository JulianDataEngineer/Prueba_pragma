-- create external table 
CREATE EXTERNAL TABLE IF NOT EXISTS pragma.cuatro( timestamp timestamp, price INT, user_id INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' WITH SERDEPROPERTIES ( 'separatorChar' = ',', 'quoteChar' = '\"') STORED AS TEXTFILE LOCATION '/user/hdfs/cuatro' tblproperties ('skip.header.line.count'='1','external.table.purge'='false') -- create ORC table 
DROP TABLE IF EXISTS pragma.cuatro_transactional;

 CREATE TABLE pragma.cuatro_transactional( timestamp timestamp, price INT, user_id INT ) STORED AS ORC TBLPROPERTIES ('transactional'='true');

 -- Insert data, external to ORC 
 INSERT OVERWRITE TABLE pragma.cuatro_transactional SELECT * FROM pragma.cuatro;

 INSERT OVERWRITE TABLE pragma.cuatro_transactional SELECT timestamp,
         price,
         user_id FROM pragma.cuatro -- validation of calculations 
SELECT *
FROM pragma.cuatro;

SELECT *
FROM pragma.cuatro_transactional;

 -- Recuento de filas cargadas a la base de datos 
SELECT COUNT(*) AS total_filas FROM pragma.cuatro_transactional;

 -- Valor medio,
         mínimo y máximo del campo "price" 
SELECT AVG(price) AS valor_medio,
         MIN(price) AS valor_minimo,
         MAX(price) AS valor_maximo
FROM pragma.cuatro_transactional;

 -- Drop tables 
DROP TABLE pragma.cuatro DROP TABLE pragma.cuatro_transactional 