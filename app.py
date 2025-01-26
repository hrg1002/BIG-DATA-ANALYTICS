from flask import Flask, jsonify, request
from flask_cors import CORS
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'cassandra'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'models'))
from store_weather_data import retrieve_daily_weather_by_date,retrieve_weather_data_by_date
from store_pollution import retrieve_pollution_data_by_date,retrieve_today_pollution_data
from predictions import preprocess_and_predict
import pandas as pd

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/api/v1/weatherData', methods=['GET'])
def endpoint1():
    result = retrieve_weather_data_by_date()
    return result

@app.route('/api/v1/pollutionIndicators', methods=['GET'])
def endpoint2():
    result = retrieve_today_pollution_data()
    result = sorted(result, key=lambda row: row.date)
    pollution_indicators = []
    for row in result:
        pollution_indicators.append({
            'date': str(row.date),
            'pm10': row.pm10,
            'pm2_5': row.pm2_5,
            'no2': row.no2,
            'so2': row.so2,
            'o3': row.o3,
            'co': row.co

        })
    return jsonify(pollution_indicators)


@app.route('/api/v1/dailyWeatherData', methods=['GET'])
def endpoint3():
    start_date = request.args.get('start_date', 'default')
    end_date = request.args.get('end_date')
    result = retrieve_daily_weather_by_date(start_date, end_date)
    result = sorted(result, key=lambda row: row.date)
    daily_weather_data = []
    for row in result:
        daily_weather_data.append({
            'date': str(row.date),
            'max_temperature': row.max_temperature,
            'min_temperature': row.min_temperature,
            'avg_temperature': row.avg_temperature
        })
    return jsonify(daily_weather_data)

@app.route('/api/v1/predictions', methods=['GET'])
def endpoint4():
    start_date = request.args.get('start_date', 'default')
    end_date = request.args.get('end_date')
    result = retrieve_daily_weather_by_date(start_date, end_date)
    result = sorted(result, key=lambda row: row.date)
    daily_weather_data = []
    for row in result:
        daily_weather_data.append({
            'date': str(row.date),
            'max_temperature': row.max_temperature,
            'min_temperature': row.min_temperature,
            'avg_temperature': row.avg_temperature
        })
    
    pollution_data = retrieve_pollution_data_by_date(start_date, end_date)
    pollution_data = sorted(pollution_data, key=lambda row: row.date)
    pollution_indicators = []
    for row in pollution_data:
        pollution_indicators.append({
            'date': str(row.date),
            'pm10': row.pm10,
            'pm2_5': row.pm2_5,
            'no2': row.no2,
            'so2': row.so2,
            'o3': row.o3,
            'co': row.co,
            "nh3": row.nh3
        })
    
    print("daily_weather_data:", len(daily_weather_data))
    print("pollution_data:", len(pollution_indicators))
    
    if len(pollution_indicators) == 0:
        response = {
            'error': 'No pollution data available for the specified date range'
        }
        return jsonify(response)
    if len(daily_weather_data) == 0:
        response = {
            'error': 'No weather data available for the specified date range'
        }
        return jsonify(response)
    
    prediction = preprocess_and_predict(pd.DataFrame(daily_weather_data), pd.DataFrame(pollution_indicators))
    if type(prediction) == str:
        response = {
            'error': 'Prediction failed'
        }
        return jsonify(response)
    
    response = {
        "prediction": prediction
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=5000)