import csv
import json
import time
from kafka import KafkaProducer

ARCHIVO = "/home/vboxuser/proyecto_ciberseguridad/UNSW_NB15_training-set.csv"
TOPICO = "network_events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda dato: json.dumps(dato).encode("utf-8")
)

enviados = 0

try:
    with open(ARCHIVO, "r", newline="", encoding="utf-8-sig") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            evento = {
                "id": int(fila["id"]),
                "proto": fila["proto"],
                "service": fila["service"],
                "state": fila["state"],
                "dur": float(fila["dur"] or 0),
                "spkts": int(fila["spkts"] or 0),
                "dpkts": int(fila["dpkts"] or 0),
                "sbytes": int(fila["sbytes"] or 0),
                "dbytes": int(fila["dbytes"] or 0),
                "attack_cat": fila["attack_cat"],
                "label": int(fila["label"])
            }

            producer.send(TOPICO, value=evento)
            enviados += 1

            if enviados % 1000 == 0:
                producer.flush()
                print(f"Eventos enviados: {enviados}")
                time.sleep(1)

    producer.flush()
    print(f"Envio finalizado. Total de eventos: {enviados}")

except Exception as error:
    print(f"Error al enviar eventos: {error}")

finally:
    producer.close()
