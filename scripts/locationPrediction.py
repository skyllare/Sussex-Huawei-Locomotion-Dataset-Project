from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, hour, minute, dayofmonth, year, month, from_unixtime
from pyspark.ml.feature import VectorAssembler
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
from sklearn.preprocessing import StandardScaler
import folium
from datetime import timedelta, datetime

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LocationPrediction") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project.Hips_Location") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project.Hips_Location") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Load data from MongoDB collection
df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# Convert 'Time' column (which is in timestamp format in milliseconds) to readable timestamp
df = df.withColumn('timestamp', (col('Time') / 1000).cast('timestamp'))  # Convert to seconds
df = df.withColumn('hour', hour(col('timestamp')))  # Extract hour
df = df.withColumn('minute', minute(col('timestamp')))  # Extract minute

# Create half-hour intervals (0, 1, 2, ..., 47) based on hour and minute
df = df.withColumn('half_hour_interval', 
                   F.floor((F.col('hour') * 60 + F.col('minute')) / 30))

# Extract additional features (year, month, day)
df = df.withColumn("day_of_week", date_format("timestamp", "u").cast("int"))
df.show(5)

# Select relevant columns
features = ['Latitude', 'Longitude', 'Altitude', 'half_hour_interval', 'day_of_week']
df_spark = df.select(*features)

# Assemble the features into a single vector column for LSTM
assembler = VectorAssembler(inputCols=['Latitude', 'Longitude', 'Altitude', 'half_hour_interval', 'day_of_week'], outputCol="features")
df_assembled = assembler.transform(df_spark)

# Handle missing values (drop rows with any missing values)
df_assembled = df_assembled.na.drop()

# Convert to Pandas DataFrame for LSTM compatibility
pandas_df = df_spark.toPandas()

# Normalize latitude and longitude
scaler = StandardScaler()
pandas_df[['Latitude', 'Longitude']] = scaler.fit_transform(pandas_df[['Latitude', 'Longitude']])


# Prepare the data for LSTM (sequence data preparation)
def create_sequences(df, seq_length):
    """Sequence data preparation for LSTM"""
    sequences = []
    labels = []
    for i in range(len(df) - seq_length):
        seq = df.iloc[i:i+seq_length][['Latitude', 'Longitude']].values
        label = df.iloc[i+seq_length][['Latitude', 'Longitude']].values
        sequences.append(seq)
        labels.append(label)
    return np.array(sequences), np.array(labels)

# Choose a sequence length for LSTM (e.g., 10 previous time steps)
SEQ_LENGTH = 10
X, y = create_sequences(pandas_df, SEQ_LENGTH)

# Split into training and testing sets (e.g., 80% training, 20% testing)
train_size = int(0.8 * len(X))
X_train, X_test = X[:train_size], X[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

# Build the LSTM model
model = Sequential([
    LSTM(50, activation='relu', input_shape=(SEQ_LENGTH, 2), return_sequences=False),
    Dense(2)  # Output 2 values (latitude, longitude)
])

model.compile(optimizer=Adam(), loss='mean_squared_error')

# Train the LSTM model
model.fit(X_train, y_train, epochs=10, batch_size=32)

# Make predictions on the test set
predictions = model.predict(X_test)

# Inverse the scaling to get the original latitude/longitude values
predictions_original = scaler.inverse_transform(predictions)


# Generate predicted locations for each half-hour of the day
def predict_for_half_hours(predicted_locations, df):
    # Merge predictions with original intervals, assume that predictions align with the half-hour intervals
    df_predictions = pd.DataFrame(predicted_locations, columns=['Predicted_Latitude', 'Predicted_Longitude'])
    
    # Add half-hour intervals from the original DataFrame
    df_predictions['half_hour_interval'] = df['half_hour_interval'].iloc[-len(df_predictions):].values
    
    return df_predictions


# Predict locations at each half-hour and display on map
predicted_locations = predict_for_half_hours(predictions_original, pandas_df)


# Visualize on a map using Folium
"""Plot the """
# Visualize on a map using Folium
def plot_on_map(predicted_df):
    # Initialize the map at the first predicted location (using the first lat/long)
    m = folium.Map(location=[predicted_df['Predicted_Latitude'][0], predicted_df['Predicted_Longitude'][0]], zoom_start=13)

    # Filter the DataFrame to include only rows where the half-hour interval changes
    predicted_df['half_hour_interval_changed'] = predicted_df['half_hour_interval'].ne(predicted_df['half_hour_interval'].shift())

    # Define the base time (start time) for the intervals (00:00)
    base_time = timedelta(hours=0, minutes=0)  # Start from 00:00

    # Add a marker for each predicted location where the half-hour interval has changed
    for index, row in predicted_df.iterrows():
        if row['half_hour_interval_changed']:
            # Calculate the predicted time for the half-hour interval
            predicted_time = base_time + timedelta(minutes=row['half_hour_interval'] * 30)
            
            # Convert timedelta to datetime for formatting
            predicted_time = datetime(1, 1, 1) + predicted_time  # Use any arbitrary date
            formatted_time = predicted_time.strftime("%I:%M %p")  # AM/PM formatting

            folium.Marker(
                location=[row['Predicted_Latitude'], row['Predicted_Longitude']],
                popup=f"Half-Hour Interval: {formatted_time}<br>Lat: {row['Predicted_Latitude']}, Long: {row['Predicted_Longitude']}",
            ).add_to(m)

    # Save the map to an HTML file
    m.save('predicted_locations_map.html')
    return m

# Plot and save map
plot_on_map(predicted_locations)
