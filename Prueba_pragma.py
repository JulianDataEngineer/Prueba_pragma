from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, count

# Inicializar Spark Session
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# Ruta de la carpeta con los archivos CSV
data_path = "/user/hdfs"

# Crear DataFrame temporal para almacenar los resultados intermedios
temp_df = spark.createDataFrame([], ["timestamp", "price", "user_id"])

# Función para procesar cada micro-batch
def process_micro_batch(file_path):
    # Cargar datos del archivo CSV en un DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Unir el DataFrame temporal con los nuevos datos
    temp_df = temp_df.union(df)

    # Calcular estadísticas
    stats = temp_df.agg(count("*").alias("row_count"),
                        avg("price").alias("avg_price"),
                        min("price").alias("min_price"),
                        max("price").alias("max_price"))

    # Mostrar estadísticas
    stats.show()

# Procesar cada archivo CSV como un micro-batch
files = ["2012-1.csv", "2012-2.csv", "2012-3.csv", "2012-4.csv", "2012-5.csv"]
for file in files:
    file_path = data_path + file
    process_micro_batch(file_path)

# Consulta de resultados después de cargar todos los archivos
final_stats = temp_df.agg(count("*").alias("total_rows"),
                           avg("price").alias("avg_price"),
                           min("price").alias("min_price"),
                           max("price").alias("max_price"))
final_stats.show()

# Ejecutar validation.csv a través del pipeline
validation_file = "validation.csv"
validation_path = data_path + validation_file
process_micro_batch(validation_path)

# Consulta de resultados después de cargar validation.csv
final_stats_after_validation = temp_df.agg(count("*").alias("total_rows"),
                                          avg("price").alias("avg_price"),
                                          min("price").alias("min_price"),
                                          max("price").alias("max_price"))
final_stats_after_validation.show()

