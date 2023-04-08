
### Setup
Clone this repository
```
git clone https://github.com/malekslokom/WeatherAppUsingDocker.git
```

Go to the directory
```
cd WeatherAppUsingDocker
```

Build the DataStax Kafka Connector image
```
docker build --no-cache -t datastax-connect -f Dockerfile-connector .
```

docker build . -t kafka-producer -f Dockerfile-producer
```

Start Zookeeper, Kafka Brokers, Kafka Connect, Cassandra, and the producer containers
```
docker-compose up -d
```

### Running
Now that everything is up and running, it's time to set up the flow of data from Kafka to Cassandra.

#### Create the Kafka topic
Start a bash shell on the Kafka Broker
```
docker exec -it kafka-broker bash
```
Create the topic
```
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic weather --config retention.ms=-1
```

#### Create the Cassandra tables
Start a cqlsh shell on the Cassandra node
```
docker exec -it cassandra cqlsh
```
Create the tables that the connector will write to. Note that a single instance of the connector can write Kafka records to multiple tables.
```
create keyspace if not exists kafka_examples with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE weatherdb.reportLondon ( lon float, lat float, weather text, weather_description text, temp float, feels_like float, temp_min float, temp_max float, pressure int, humidity int, visibility int, wind_speed float, rain1h float, clouds int, dt bigint, sys_type int, sys_id int, sys_country text, sys_sunrise bigint, sys_sunset bigint, timezone int, id int, name text,time text, PRIMARY KEY (time) );
```



#### Start start sink endPoint
Execute the following command from the machine where docker is running to start the connector using the Kafka Connect REST API
```
curl -X POST -H "Content-Type: application/json" -d @connector-config.json "http://localhost:8083/connectors"
```

restart sink :
curl -X POST "http://worker_ip:rest_port/connectors/connector/restart"
delete sink :
curl -X DELETE "http://worker_ip:port/connectors/connector"

#### Confirm rows written to Cassandra
Start a cqlsh shell on the Cassandra node
```
docker exec -it cassandra cqlsh
```

Confirm rows were written to each of the Cassandra tables
```
select * from weatherdb.reportlondon;
```
