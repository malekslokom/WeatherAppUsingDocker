import time
import json
import requests
import os
import datetime
from kafka import KafkaProducer

# Configuration des serveurs Kafka
bootstrap_servers =['localhost:9092'] #['broker:9092']


# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))
print(producer)
try:
    # Envoyer les données au topic Kafka
    x =  '{ "time":"2023-03-23 08:48:06", "description":"cloud"}'
    data=json.loads(x)
    producer.send("weather", data)
    print(f'Sent weather data to Kafka: {data}')

except Exception as e:
    print("aaaaa")
    print(f'Error retrieving or sending weather data to Kafka: {e}')

