import time
import json
import requests
import os
import datetime
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("WEATHER_TOPIC")
print(KAFKA_BROKER_URL)
# SLEEP_TIME = int(os.environ.get("SLEEP_TIME", "60"))


# Configuration des serveurs Kafka
bootstrap_servers = ['broker:29092'] #['localhost:9092']

# Configuration de l'API OpenWeatherMap
api_key = 'cf855657d928f11ff713b34d631de653'
CITY_NAMES = ['London', 'Liverpool', 'Manchester','Paris','Nantes','Toulouse','Amsterdam','Rotterdam','Delft','Tunisia']
# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    try:
        for city in CITY_NAMES:
            print(city)
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            print(url)
            # Obtenir les données météorologiques
            response = requests.get(url) 
            data = response.json()
            print(data)
            lon=data['coord']['lon']
            lat=data['coord']['lat']
            print(lon)
            url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}'
            resp = requests.get(url).json()
            print(resp)
            topic_name = f"weather-forecast-{city.lower()}"
            print(topic_name)
            # Envoyer les données à Kafka
            producer.send(topic_name, value=resp)
            print('data sent')
        # Attendre avant d'envoyer les données suivantes
        time.sleep(60)
        

    except Exception as e:
        print(f'Error retrieving or sending weather data to Kafka: {e}')
