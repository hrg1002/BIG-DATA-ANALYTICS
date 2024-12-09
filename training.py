import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import pandas as pd
import matplotlib.pyplot as plt

columns = ["date", "tavg", "tmin", "tmax", "prcp", "wdir", "wspd", "pres", "pm25", "pm10", "o3", "no2", "so2", "co", "demand"]
start_date = '2023-02-01'
end_date = '2024-12-01'

weather = pd.read_csv('data/weather_data.csv')
pollution = pd.read_csv('data/polution_data_el_bosque.csv')
demand = pd.read_csv('data/demand.csv')

# Preprocess the data
weather['date'] = pd.to_datetime(weather['date'])
pollution['date'] = pd.to_datetime(pollution['date'])
demand['date'] = pd.to_datetime(demand['date'], unit='s')

data = pd.merge(weather, pollution, on='date', how='inner')
data = pd.merge(data, demand, on='date', how='inner')
data = data[columns]

data = data.loc[(data['date'] > start_date) & (data['date'] <= end_date)]
del data['date']

data = data.fillna(0)

# Normalize the data (excluding the target column 'demand')
scaler = MinMaxScaler()
normalized_data = scaler.fit_transform(data.drop('demand', axis=1))

# Normalize the target column separately
demand_scaler = MinMaxScaler()
normalized_demand = demand_scaler.fit_transform(data[['demand']])

# Add the normalized target column to the input features
normalized_data = np.column_stack((normalized_data, normalized_demand))

# Create sliding window sequences
def create_sequences(data, sequence_length):
    sequences = []
    for i in range(len(data) - sequence_length):
        sequences.append((data[i:i + sequence_length, :-1], data[i + sequence_length, -1]))  # Last column is the target
    return sequences

# Create sequences with a length of 10
sequence_length = 10
sequences = create_sequences(normalized_data, sequence_length)

# Separate into X (input) and y (output)
X, y = zip(*sequences)
X = np.array(X)
y = np.array(y)

# Check shape of input data
print(X.shape)  # Should be (num_samples, sequence_length, num_features)
print(y.shape)  # Should be (num_samples, )

# Build the LSTM model
model = Sequential([
    LSTM(50, activation='relu', input_shape=(X.shape[1], X.shape[2])),  # X.shape[1] = sequence_length, X.shape[2] = num_features
    Dense(1)  # Output layer
])

# Compile the model
model.compile(optimizer='adam', loss='mse')

# Train the model
model.fit(X, y, epochs=20, batch_size=32)

# Make predictions
predictions = model.predict(X)

# Inverse transform the predictions and actual values to the original scale
# Reverse the normalization for the target (demand)
y_inverse = demand_scaler.inverse_transform(y.reshape(-1, 1))

# Reverse the normalization for the predictions (demand)
predictions_inverse = demand_scaler.inverse_transform(predictions)

# Plot the results
plt.figure(figsize=(10, 6))
plt.plot(y_inverse, label='Actual values')
plt.plot(predictions_inverse, label='Predicted values', linestyle='dashed')
plt.title('LSTM Model Predictions vs Actual Values')
plt.xlabel('Time')
plt.ylabel('Demand')
plt.legend()

# Save the plot as an image
plt.savefig("images/lstm_predictions_plot.png")

# Display the plot
plt.show()

# Save the model
model.save("models/lstm_model.keras")