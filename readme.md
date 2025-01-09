# Analysisi of the correlation of Weather and pollution data with respiratory issues
This projects analyzes the correlatino between the weather conditions and pollution indicators in Chile on real-time and predict medical visits
## Usage
- pip install -r requirements.txt
### activate kafka server
- Docker compose up
### read data from open databases
- python3 weather_data.py
- python3 spark_kafka_consumer.py 
### consume and process data
- python3 spark_kafka_consumer
- python3 pollution_data_consumer

If doesnt work,stop containers and do docker compose up again 
Ctrl+C to stop