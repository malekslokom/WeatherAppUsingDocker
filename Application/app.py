import sys
sys.path.insert(0, '/Users/malekslokom/Desktop/2ING/weather-app/WeatherAppUsingDocker/App/weather/lib/python3.8/site-packages')
import joblib
import numpy as np
import json
import requests
from flask import request
from flask import Flask, render_template, request
from flask import Flask
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from kafka import KafkaConsumer
import pandas as pd

import datetime
import plotly.express as px
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(['localhost'], auth_provider=auth_provider)
session = cluster.connect('weatherdb')
print(session)

KAFKA_BROKER_URL="localhost:9092"
app = Flask(__name__)

@app.route("/")
def hello(): 
  return render_template('index.html')   

@app.route("/currentWeatherData/<city>",)
def currentWeatherDataOfSpecificCity(city):
  TOPIC_NAME="weather-"+city
  consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_BROKER_URL],api_version=(0, 10, 1),auto_offset_reset='latest')
  print('Waiting for msg...')
  for msg in consumer:
    print('got one!')
    msg = msg.value.decode('ascii')
    print(msg)
    jsonData=json.loads(msg)
    return jsonData



@app.route("/forecastWeatherData/<city>")
def forecastWeatherDataOfSpecificCity(city):
  api_key= 'b4e44ae423a1d68110e989819ca31261'

  #get city info
  # url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric' 
  # response = requests.get(url).json()
  # print(response)
  # #forecast data
  # lon=response['coord']['lon']
  # lat=response['coord']['lat']
  # url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}'
  # resp = requests.get(url).json()
  # print(resp)
  # return resp
  consumer = KafkaConsumer("weather-forecast-london", bootstrap_servers=[KAFKA_BROKER_URL],api_version=(0, 10, 1),auto_offset_reset='latest')
  print('Waiting for msg (forecast)...')
  for msg in consumer:
    print('got one!')
    msg = msg.value.decode('ascii')
    print(msg)
    jsonData=json.loads(msg)
    return jsonData


@app.route('/allhistory', methods=['GET'])
def get():
    rows = session.execute('SELECT * FROM reportlondon')
    result = []
    for row in rows:
        result.append({
            'city': row.name,
            'humidity': row.humidity,
            'temp': row.temp,
            'time': row.time,
            'id':row.id
        })

    return result

@app.route('/prediction', methods=['GET'])
def prediction():
    model = joblib.load(open('/Users/malekslokom/Desktop/2ING/weather-app/WeatherAppUsingDocker/App/finalized_model.pkl','rb'))
    print(model)
    #precipitation ,temp_max ,temp_min ,wind     
    data = np.array([[104,18, 30, 23.603016]])
    prediction = model.predict(data)
    weather_dict = {0: 'drizzle', 2: 'rain', 4: 'sun', 3: 'snow', 1: 'fog'}
    print(weather_dict[prediction[0]])

    return weather_dict[prediction[0]]


@app.route('/realDataMap', methods=['GET'])
def mapLoad():
    cities = pd.DataFrame({
        'City': ['London', 'Liverpool', 'Manchester', 'Paris', 'Nantes', 'Toulouse', 'Amsterdam', 'Rotterdam', 'Delft'],
        'Latitude': [52.370216,52.011577,53.408371, 51.507351, 53.480759, 47.218371,48.856613,51.92442,  43.604652],
        'Longitude': [4.895168, 4.357068, -2.991573, -0.127758, -2.242631, -1.553621, 2.352222, 4.477733,1.444209]
    })
    cities['Coordinates'] = list(zip(cities.Longitude, cities.Latitude))
    cities['Coordinates'] = cities['Coordinates'].apply(Point)
    #metric 
    temperatures = []
    humidity = []

    for city in cities['City']:
      data=currentWeatherDataOfSpecificCity(city)
      temperatures.append(data.temp)
      humidity.append(data.humidity)
    print(temperatures)
    print(humidity)

    # #getting temp and humidity from database
    # rows = session.execute('SELECT * FROM history')
    # for row in rows:
    #     temperatures.append(row.temperature)
    #     humidity.append(row.humidity)



    geo_df = gpd.GeoDataFrame(cities, geometry='Coordinates')

    px.set_mapbox_access_token("pk.eyJ1Ijoic3R1ZHkxOTk5IiwiYSI6ImNrZGFmcTVyaTBkb3oycG16Z2JvNWVnYXgifQ.KRT-UOiIZsCBEYLHgGO7HQ")
    fig = px.scatter_mapbox(geo_df,
                        lat=geo_df.geometry.y,
                        lon=geo_df.geometry.x,
                        hover_name=cities.apply(lambda x: f"{x['City']}: { round(temperatures[x.name] ,2) } °C", axis=1),
                        size=cities.apply(lambda x: round(temperatures[x.name]), axis=1),
                        color_continuous_scale=px.colors.sequential.Redor,
                        color=cities.apply(lambda x: round(temperatures[x.name]), axis=1),
                        mapbox_style='light',
                        zoom=3)
    fig.update_layout(height=None,width=None,margin={"r":0,"t":0,"l":0,"b":0})
    fig2 = px.scatter_mapbox(geo_df,
                        lat=geo_df.geometry.y,
                        lon=geo_df.geometry.x,
                        hover_name=cities.apply(lambda x: f"{x['City']}: {humidity[x.name]} %", axis=1),
                        size=humidity,
                        color_continuous_scale=px.colors.sequential.Blues,
                        color=humidity,
                        mapbox_style='light',
                        zoom=3)
    fig2.update_layout(height=None,width=None,margin={"r":0,"t":0,"l":0,"b":0})

    
    #tunis current weather
    url = f'https://api.openweathermap.org/data/2.5/weather?q=tunis&appid=b4e44ae423a1d68110e989819ca31261&units=metric' #
    resp = requests.get(url).json()

    current_time = datetime.datetime.now()
    one_hour = datetime.timedelta(hours=1)
    new_time = current_time + one_hour

    formatted_time = new_time.strftime("%I:%M %p")
    formatted_date = current_time.strftime("%A, %d %B %Y")
    tunisObj ={
        "description" : resp['weather'][0]['description'],
        "temp" : resp['main']['temp'],
        "curtime" : formatted_time,
        "curdate" : formatted_date
    }


    return render_template('mapLoad.html', temp_plot=fig.to_html(),humi_plot=fig2.to_html(),tunisObj=tunisObj)


if __name__ == "__main__":
  app.run(port=5002,debug=True)