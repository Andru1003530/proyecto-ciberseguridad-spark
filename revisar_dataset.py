
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RevisionDatasetUNSWNB15")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "file:///home/vboxuser/proyecto_ciberseguridad/UNSW_NB15_training-set.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(ruta)
)

print("\n=== COLUMNAS DEL DATASET ===")
print(df.columns)

print("\n=== ESQUEMA DE LOS DATOS ===")
df.printSchema()

print("\n=== PRIMEROS 10 REGISTROS ===")
df.show(10, truncate=False)

print("\n=== TOTAL DE REGISTROS ===")
print(df.count())

spark.stop()
