<<<<<<< HEAD
# Analysisi of the correlation of Weather and pollution data with respiratory issues
This projects analyzes the correlatino between the weather conditions and pollution indicators in Chile on real-time and predict medical visits
=======
# Analysis of the correlation of Weather and pollution data with respiratory issues
This projects analyzes the correlation  between the weather conditions and pollution indicators in Chile on real-time and predict medical visits
>>>>>>> 01b24d3a58d446248a084a5b4709d31102cadcc7
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
<<<<<<< HEAD
Ctrl+C to stop
=======
>>>>>>> 01b24d3a58d446248a084a5b4709d31102cadcc7
