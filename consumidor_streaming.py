
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType, IntegerType
)

# Esquema de los eventos de red
esquema = StructType([
    StructField("id", LongType(), True),
    StructField("proto", StringType(), True),
    StructField("service", StringType(), True),
    StructField("state", StringType(), True),
    StructField("dur", DoubleType(), True),
    StructField("spkts", LongType(), True),
    StructField("dpkts", LongType(), True),
    StructField("sbytes", LongType(), True),
    StructField("dbytes", LongType(), True),
    StructField("attack_cat", StringType(), True),
    StructField("label", IntegerType(), True)
])

spark = (
    SparkSession.builder
    .appName("AnalisisCiberseguridadStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lectura de eventos desde Kafka
eventos_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "network_events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 10000)
    .load()
)

# Conversión del mensaje JSON a columnas
eventos = (
    eventos_kafka
    .select(
        from_json(
            col("value").cast("string"),
            esquema
        ).alias("evento")
    )
    .select("evento.*")
    .filter(col("id").isNotNull())
)

# Conteo de eventos y detección de tráfico etiquetado como ataque
resumen = (
    eventos
    .groupBy("label")
    .agg(count("*").alias("total_eventos"))
)

consulta = (
    resumen.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option(
    "checkpointLocation",
    "file:///home/vboxuser/proyecto_ciberseguridad/checkpoint_streaming"
)
    .start()
)

print("Spark Streaming conectado a Kafka.")
print("Procesando eventos de red. Detén con Ctrl+C cuando quieras.")

consulta.awaitTermination()
