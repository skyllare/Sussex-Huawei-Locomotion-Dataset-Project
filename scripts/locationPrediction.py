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
def plot_on_map(predicted_df):
    # Initialize the map at the first predicted location (using the first lat/long)
    m = folium.Map(location=[predicted_df['Predicted_Latitude'][0], predicted_df['Predicted_Longitude'][0]], zoom_start=13)

    # Filter the DataFrame to include only rows where the half-hour interval changes
    predicted_df['half_hour_interval_changed'] = predicted_df['half_hour_interval'].ne(predicted_df['half_hour_interval'].shift())
    
    # Add a marker for each predicted location where the half-hour interval has changed
    for index, row in predicted_df.iterrows():
        if row['half_hour_interval_changed']:
            folium.Marker(
                location=[row['Predicted_Latitude'], row['Predicted_Longitude']],
                popup=f"Half-Hour Interval: {row['half_hour_interval']}<br>Lat: {row['Predicted_Latitude']}, Long: {row['Predicted_Longitude']}",
            ).add_to(m)

    # Save the map to an HTML file
    m.save('predicted_locations_map.html')
    return m


# Plot and save map
plot_on_map(predicted_locations)






# # Prepare the feature matrix (X) and target vectors (y_lat and y_lon)
# X = np.array([x.toArray() for x in pandas_df["features"]])
# y_lat = pandas_df["Latitude"].values  # Latitude as target
# y_lon = pandas_df["Longitude"].values  # Longitude as target

# # Convert to DMatrix (XGBoost's internal data structure)
# dtrain_lat = xgb.DMatrix(X, label=y_lat)
# dtrain_lon = xgb.DMatrix(X, label=y_lon)

# # Set XGBoost parameters (you can tune these)
# params = {
#     "objective": "reg:squarederror",  # Regression task
#     "max_depth": 6,
#     "eta": 0.1,
#     "eval_metric": "rmse"
# }

# # Train XGBoost models
# bst_lat = xgb.train(params, dtrain_lat, num_boost_round=100)
# bst_lon = xgb.train(params, dtrain_lon, num_boost_round=100)

# # Create a function to predict latitude and longitude at future half-hour intervals
# def predict_at_intervals(model_lat, model_lon, start_time, num_intervals):
#     predictions = []
#     for i in range(num_intervals):
#         # Generate the future half-hour interval
#         current_time = start_time + pd.Timedelta(minutes=i * 30)
#         hour = current_time.hour
#         minute = current_time.minute
#         half_hour_interval = (hour * 60 + minute) // 30
        
#         day_of_week = current_time.dayofweek + 1
#         # Features: use a constant value for Latitude, Longitude, Altitude, and other time-based features
#         # Assuming constant altitude for now, adjust accordingly based on your dataset
        
#         features = np.array([0, 0, 146, half_hour_interval, day_of_week])
        
#         # Create DMatrix for prediction
#         dtest = xgb.DMatrix(features.reshape(1, -1))
        
#         # Get predictions for latitude and longitude
#         lat_pred = model_lat.predict(dtest)
#         lon_pred = model_lon.predict(dtest)
        
#         predictions.append((current_time, lat_pred[0], lon_pred[0]))
    
#     return predictions

# # Predict every half hour for the next 48 half-hour intervals (one day)
# start_time = pd.Timestamp("2024-11-17 00:00:00")
# predictions = predict_at_intervals(bst_lat, bst_lon, start_time, 48)

# # Print predictions
# for pred in predictions:
#     print(f"Time: {pred[0]}, Latitude: {pred[1]}, Longitude: {pred[2]}")

# # Train XGBoost models for Latitude and Longitude
# # Train Latitude model
# model_lat = xgb.train(params, dtrain_lat, num_boost_round=100)

# # Train Longitude model
# model_lon = xgb.train(params, dtrain_lon, num_boost_round=100)

# # Make predictions using the trained XGBoost models
# pred_lat = model_lat.predict(dtrain_lat)
# pred_lon = model_lon.predict(dtrain_lon)

# # Combine the predictions into a DataFrame
# predictions_df = pd.DataFrame({
#     'pred_lat': pred_lat,
#     'pred_lon': pred_lon
# })

# # Show predictions
# print(predictions_df.head())

# # Optionally, convert predictions back to Spark DataFrame for further processing
# predictions_spark = spark.createDataFrame(predictions_df)

# # Show the Spark DataFrame with predictions
# predictions_spark.show(1000)











# Train the XGBoost model
# num_round = 100
# bst = xgb.train(params, dtrain, num_round)

# # Make predictions
# predictions = bst.predict(dtrain)

# # Print out some of the predictions to see the results
# print(predictions[:10])



# df_assembled.select("features").show(5) 
# # normalize data 
# scaler = SparkStandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
# scaler_model = scaler.fit(df_assembled)
#  # To check if 'features' is correctly formed
# print("Test5")
# scaled_df = scaler_model.transform(df_assembled)

# # convert spark to pandas dataframe 
# scaled_features = scaled_df.select("scaled_features").rdd.map(lambda row: row[0].toArray()).collect()
# # pandas_df = df_assembled.select("features").toPandas()
# df_pandas = pd.DataFrame(scaled_features, columns=features)

# # X = np.array(pandas_df["features"].tolist())
# X = np.array(df_pandas[features].values)

# params = {
#     "objective": "reg:squarederror",
#     "max_depth": 6,
#     "eta": 0.1,
#     "eval_metric": "rmse"
# }


# dtrain = xgb.DMatrix(X)
# model = xgb.train(params, dtrain, num_boost_round=100)
# print("test6")

# predictions = model.predict(dtrain)
# print(f"Predictions: {predictions[:10]}")
# # # Assemble data for training
# df_assembled = assembler.transform(df)

# # Convert Spark DataFrame to Pandas DataFrame for XGBoost
# pandas_df = df_assembled.select("features", "Latitude", "Longitude").toPandas()

# # Prepare data for XGBoost (splitting features and target)
# X = np.array(pandas_df["features"].tolist())
# y_lat = np.array(pandas_df["Latitude"])
# y_lon = np.array(pandas_df["Longitude"])

# # # # Train XGBoost models for Latitude and Longitude separately
# dtrain_lat = xgb.DMatrix(X, label=y_lat)
# dtrain_lon = xgb.DMatrix(X, label=y_lon)

# print("TEST")
# # # # Set XGBoost parameters
# # # params = {
# # #     "objective": "reg:squarederror",
# # #     "max_depth": 6,
# # #     "eta": 0.1,
# # #     "eval_metric": "rmse"
# # # }

# # # # Train XGBoost models
# model_lat = xgb.train(params, dtrain_lat, num_boost_round=100)
# model_lon = xgb.train(params, dtrain_lon, num_boost_round=100)

# # Save the models
# model_lat.save_model("lat_model.xgb")
# model_lon.save_model("lon_model.xgb")

# # Create a prediction function
# def predict_location(model_lat, model_lon, features):
#     dtest = xgb.DMatrix(features)
#     pred_lat = model_lat.predict(dtest)
#     pred_lon = model_lon.predict(dtest)
#     return pred_lat, pred_lon

# # Example of predicting the next location
# pred_lat, pred_lon = predict_location(model_lat, model_lon, X[:1])

# print(f"Predicted Latitude: {pred_lat[0]}, Predicted Longitude: {pred_lon[0]}")
