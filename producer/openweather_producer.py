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
CITY_NAMES = ['London', 'Liverpool', 'Manchester','Paris','Nantes','Toulouse','Amsterdam','Rotterdam','Delft']
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
            newData={}
            newData['lon']=data['coord']['lon']
            newData['lat']=data['coord']['lat']
            newData['weather']=data['weather'][0]['main']
            newData['weatherDescription']=data['weather'][0]['description']
            newData['temp']=data['main']['temp']
            newData['feels_like']=data['main']['feels_like']
            newData['temp_min']=data['main']['temp_min']
            newData['temp_max']=data['main']['temp_max']
            newData['pressure']=data['main']['pressure']
            newData['humidity']=data['main']['humidity']
            newData['visibility']=data['visibility']
            newData['windSpeed']=data['wind']['speed']
            #newData['rain1h']=data['rain']['1h']
            newData['clouds']=data['clouds']['all']
            newData['dt']=data['dt']
            newData['sysType']=data['sys']['type']
            newData['sysId']=data['sys']['id']
            newData['sysCountry']=data['sys']['country']
            newData['sysSunrise']=data['sys']['sunrise']
            newData['sysSunset']=data['sys']['sunset']
            newData['timezone']=data['timezone']
            newData['id']=data['id']
            newData['name']=data['name']
            now = datetime.now()
            newData['time']=now.strftime("%m/%d/%y %H:%M:%S")
            print(newData)
            # x =  '{ "time":"2023-03-23 08:48:06", "description":"cloud"}'
            # data=json.loads(x)
            # print(type(data))
            # print(type(newData))
            #now = datetime.datetime.now()
        # formatted_date = now.strftime("%Y-%m-%d %H:%M:%S")
        # data["time"]=formatted_date
        
            # # Envoyer les données au topic Kafka
            # producer.send("weather", newData)
            # print(f'Sent weather data to Kafka: {newData}')
            # # Attendre 15 minutes avant de récupérer les données à nouveau
            topic_name = f"weather-{city.lower()}"
            print(topic_name)
            # Envoyer les données à Kafka
            producer.send(topic_name, value=newData)
            print('data sent')
        # Attendre avant d'envoyer les données suivantes
        time.sleep(60)
        

    except Exception as e:
        print("aaaaa")
        print(f'Error retrieving or sending weather data to Kafka: {e}')
