from pyspark.sql import SparkSession
from pyspark.sql.functions import count, expr

# Inicializar Spark Session
spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# Ruta de la carpeta con los archivos CSV
data_path = "/ruta/de/tu/carpeta/datos/"

# Crear DataFrame temporal para almacenar los resultados intermedios
temp_df = spark.createDataFrame([], ["timestamp", "price", "user_id"])

# Inicializar estadísticas
row_count = 0
avg_price = 0.0
min_price = float('inf')
max_price = float('-inf')

# Función para procesar cada micro-batch
def process_micro_batch(file_path):
    global row_count, avg_price, min_price, max_price

    # Cargar datos del archivo CSV en un DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    for row in df.rdd.collect():
        # Actualizar estadísticas después de cada fila
        row_count += 1
        price = row['price']
        avg_price = ((avg_price * (row_count - 1)) + price) / row_count
        min_price = min(min_price, price)
        max_price = max(max_price, price)

        # Mostrar estadísticas en ejecución
        print(f"Estadísticas en ejecución - Fila {row_count}:")
        print(f"Total de filas: {row_count}")
        print(f"Valor promedio: {avg_price}")
        print(f"Valor mínimo: {min_price}")
        print(f"Valor máximo: {max_price}\n")

# Procesar cada archivo CSV como un micro-batch
files = ["2012-1.csv", "2012-2.csv", "2012-3.csv", "2012-4.csv", "2012-5.csv"]
for file in files:
    file_path = data_path + file
    process_micro_batch(file_path)

# Resultados después de cargar todos los archivos
print("\nResultados después de cargar todos los archivos:")
print(f"Total de filas: {row_count}")
print(f"Valor promedio: {avg_price}")
print(f"Valor mínimo: {min_price}")
print(f"Valor máximo: {max_price}\n")

# Ejecutar validation.csv a través del pipeline
validation_file = "validation.csv"
validation_path = data_path + validation_file
process_micro_batch(validation_path)

# Resultados después de cargar validation.csv
print("\nResultados después de cargar validation.csv:")
print(f"Total de filas: {row_count}")
print(f"Valor promedio: {avg_price}")
print(f"Valor mínimo: {min_price}")
print(f"Valor máximo: {max_price}\n")
