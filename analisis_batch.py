
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc

spark = (
    SparkSession.builder
    .appName("AnalisisBatchCiberseguridad")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ruta = "file:///home/vboxuser/proyecto_ciberseguridad/UNSW_NB15_training-set.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("nullValue", "")
    .csv(ruta)
)

print("\n=== TOTAL DE REGISTROS ORIGINALES ===")
print(df.count())

df = df.dropDuplicates()
df = df.filter(col("label").isNotNull())

print("\n=== TOTAL DE REGISTROS ANALIZADOS ===")
print(df.count())

print("\n=== TRAFICO NORMAL VS. ATAQUES ===")
df.groupBy("label").agg(
    count("*").alias("total_registros")
).orderBy("label").show()

print("\n=== CATEGORIAS DE ATAQUES ===")
df.filter(
    col("attack_cat").isNotNull()
    & (col("attack_cat") != "")
    & (col("attack_cat") != "Normal")
).groupBy("attack_cat").agg(
    count("*").alias("total_registros")
).orderBy(desc("total_registros")).show(
    30, truncate=False
)

print("\n=== PROTOCOLOS MAS FRECUENTES ===")
df.groupBy("proto").agg(
    count("*").alias("total_registros")
).orderBy(desc("total_registros")).show(
    20, truncate=False
)

print("\n=== ESTADISTICAS DEL TRAFICO ===")
df.select(
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate"
).summary().show()

print("\n=== COMPARACION ENTRE TRAFICO NORMAL Y ATAQUES ===")
df.groupBy("label").agg(
    count("*").alias("registros"),
    avg("dur").alias("duracion_promedio"),
    avg("sbytes").alias("bytes_origen_promedio"),
    avg("dbytes").alias("bytes_destino_promedio"),
    avg("rate").alias("tasa_promedio")
).orderBy("label").show(truncate=False)

spark.stop()

print("\nAnalisis por lotes finalizado correctamente.")

