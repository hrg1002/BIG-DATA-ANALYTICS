import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
# Description: Preprocess the data for making the predictions
#steps
#1.Take the pollution data and weather_daily data
#2.Merge the pollution and weather data by date
#3. Remove the values where there are missing data and the innecessary fileds,which are id,lat,lon.
#4Transform data into numerical fromat
#4 Add fields wdir and wspeed with random valeis(wdir between 0 and 360)
#4.Reshape the data for make the format for the correct format
def create_sequences(data, sequence_length):
    sequences = []
    for i in range(len(data) - sequence_length):
        sequences.append((data[i:i + sequence_length, :-1], data[i + sequence_length - 1, -1]))  # Last column is the target
        print(sequences)
    return sequences

# Create sequences with a length of 10
def process_data(pollution_data, weather_data):
    # Merge the data column-wise
    merged_data = pd.merge(weather_data, pollution_data, on='date')
    
    # Remove unnecessary fields
    
    # Remove rows with missing data
    merged_data = merged_data.dropna()
    
    # Transform the date field into a numerical format
    merged_data['date'] = pd.to_datetime(merged_data['date']).astype(int) / 10**9  # Convert to seconds since epoch
    
    # Add fields wdir and wspeed with random values
    merged_data['wdir'] = np.random.uniform(0, 360, merged_data.shape[0])
    merged_data['wspeed'] = np.random.uniform(0, 100, merged_data.shape[0])
    
    # Ensure there are enough rows to reshape
    if merged_data.shape[0] < 10:
        raise ValueError("Not enough data to create sequences for prediction.")
    
    # Reshape the data to the correct format
    sequence = merged_data.to_numpy()
    scaler = MinMaxScaler()
    scaler.fit(sequence)
    normalized_data = scaler.transform(sequence)
    sequences = normalized_data.reshape(-1, 10, 13)
    
    return np.array(sequences)

