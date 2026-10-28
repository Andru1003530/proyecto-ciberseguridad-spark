# Proyecto de Ciberseguridad: Análisis de Tráfico de Red con Spark y Kafka

## Descripción

Este proyecto implementa un análisis de tráfico de red utilizando el conjunto de datos UNSW-NB15, con el objetivo de identificar y analizar registros de tráfico normal y registros asociados a ataques informáticos.

Se utilizan Apache Spark y PySpark para el procesamiento por lotes (Batch), y Apache Kafka junto con Spark Structured Streaming para el procesamiento de eventos en tiempo real.

## Tecnologías utilizadas

- Ubuntu Linux
- Java 11
- Python 3.12
- Apache Spark 3.5.8
- Apache Kafka 3.9.2
- PySpark
- Git y GitHub

## Archivos del proyecto

- `analisis_batch.py`: realiza el procesamiento por lotes y el análisis estadístico del tráfico de red.
- `productor_kafka.py`: publica los registros del conjunto de datos en el tópico de Kafka `network_events`.
- `consumidor_streaming.py`: consume y procesa los eventos mediante Spark Structured Streaming.
- `revisar_dataset.py`: permite revisar la estructura y el contenido del conjunto de datos.
- `.gitignore`: excluye archivos y directorios locales que no deben publicarse en el repositorio.

## Conjunto de datos

Se utiliza UNSW-NB15 para analizar tráfico normal y tráfico asociado a diferentes tipos de ataques.

El archivo CSV original no se incluye en este repositorio. Debe obtenerse desde una fuente autorizada y ubicarse en el directorio raíz del proyecto con el nombre:

`UNSW_NB15_training-set.csv`

## Resultados del análisis por lotes

El procesamiento analizó 175.341 registros:

- Tráfico normal (`label = 0`): 56.000 registros (31,93 %).
- Tráfico asociado a ataques (`label = 1`): 119.341 registros (68,07 %).

Entre las categorías identificadas se encuentran Generic, Exploits, Fuzzers, DoS y Reconnaissance.

## Requisitos previos

Antes de ejecutar el proyecto, se debe contar con Java 11, Python, Apache Spark, Apache Kafka y las dependencias de Python necesarias. También se debe configurar el entorno virtual e instalar las librerías requeridas.

## Ejecución del procesamiento por lotes

Desde el directorio del proyecto, ejecutar:

```bash
/opt/spark/bin/spark-submit analisis_batch.py
```

## Ejecución del procesamiento con Kafka y Spark Streaming

1. Iniciar Kafka en modo KRaft según la configuración local.
2. Crear o verificar el tópico `network_events`.
3. Ejecutar el productor para enviar los eventos.
4. Iniciar el consumidor de Spark Structured Streaming.

Los comandos específicos dependen de la configuración local de Kafka y de las dependencias instaladas.

## Objetivo académico

Demostrar el uso de herramientas de procesamiento distribuido y streaming para analizar tráfico de red, reconocer patrones y estudiar la distribución de registros normales y asociados a ataques informáticos.
