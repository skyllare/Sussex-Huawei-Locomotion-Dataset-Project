from datetime import timedelta, datetime
import numpy as np
import pandas as pd
import tensorflow as tf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, unix_timestamp, hour, minute, dayofmonth, year, month, from_unixtime, lit, date_format
from pyspark.ml.feature import VectorAssembler
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam
from pymongo import MongoClient  # Added for MongoDB connection
import scripts.uploadPredictedLocations as upload

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
    """
    Prepare sequence data for LSTM by splitting into sequences and labels.
    
    Args:
        df: DataFrame with the features (Latitude, Longitude).
        seq_length: Length of each sequence to be used for training.
        
    Returns:
        Tuple: Sequences and corresponding labels as numpy arrays.
    """
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
    """
    Merge predicted locations with the original half-hour intervals.
    
    Args:
        predicted_locations: Predicted latitude and longitude values.
        df: Original DataFrame containing half-hour intervals.
        
    Returns:
        DataFrame with predicted locations and intervals.
    """
    # Merge predictions with original intervals, assume that predictions align with the half-hour intervals
    df_predictions = pd.DataFrame(predicted_locations, columns=['Predicted_Latitude', 'Predicted_Longitude'])
    
    # Add half-hour intervals from the original DataFrame
    df_predictions['half_hour_interval'] = df['half_hour_interval'].iloc[-len(df_predictions):].values
    
    return df_predictions

# Predict locations at each half-hour
predicted_locations = predict_for_half_hours(predictions_original, pandas_df)

# Upload predicted locations to MongoDB
upload.upload_predicted_locations_to_mongo(predicted_locations)
