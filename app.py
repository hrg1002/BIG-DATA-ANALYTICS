from flask import Flask, jsonify, request
from flask_cors import CORS
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'cassandra'))
from store_weather_data import retrieve_daily_weather_by_date

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

@app.route('/api/v1/weatherData', methods=['GET'])
def endpoint1():
    response = {
        "message": "This is the response from endpoint 1",
        "status": "success"
    }
    return jsonify(response)

@app.route('/api/v1/pollutionIndicators', methods=['GET'])
def endpoint2():
    response = {
        "message": "This is the response from endpoint 2",
        "data": [1, 2, 3, 4]
    }
    return jsonify(response)

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
    response = {
        "message": "Welcome to endpoint 4",
        "success": True
    }
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
