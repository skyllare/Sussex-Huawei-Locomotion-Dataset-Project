import pandas as pd
import pymongo
from pymongo import MongoClient
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.ml.feature import StandardScaler as SparkStandardScaler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col
from pyspark.ml.classification import OneVsRest
from pyspark.ml.clustering import KMeans

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.get_database("Project")
collection = db['Hips_Motion']

# Initialize Spark session
spark = SparkSession.builder \
    .appName("IsolationForestAnomalyDetection") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project.Hips_Motion") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project.Hips_Motion") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df_spark = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

# features to use -- found anomalies in these 
features = [
    'AccelerationX', 'AccelerationY', 'AccelerationZ',
    'GyroscopeX', 'GyroscopeY', 'GyroscopeZ',
    'GravityX', 'GravityY', 'GravityZ',
    'Pressure'
]

df_spark = df_spark.select(*features)
# df_spark = df_spark.limit(1000) # limits data for testing measures 

# Vectorize the features for use in Spark's StandardScaler
assembler = VectorAssembler(inputCols=features, outputCol="features")
vectorized_df = assembler.transform(df_spark)

# normalize data 
scaler = SparkStandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=False)
scaler_model = scaler.fit(vectorized_df)
scaled_df = scaler_model.transform(vectorized_df)

# convert spark to pandas dataframe 
scaled_features = scaled_df.select("scaled_features").rdd.map(lambda row: row[0].toArray()).collect()
df_pandas = pd.DataFrame(scaled_features, columns=features)

# Train Isolation Forest using scikit-learn
model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
model.fit(df_pandas)

# Get anomaly predictions
anomaly_scores = model.decision_function(df_pandas)
predictions = model.predict(df_pandas)

# Visualize anomaly scores
plt.hist(anomaly_scores, bins=50)
plt.title("Anomaly Scores")
plt.xlabel("Anomaly Score")
plt.ylabel("Frequency")
plt.show()

# Filter anomalies (predictions == -1 means anomaly)
anomalies = df_spark.rdd.zipWithIndex().filter(lambda x: predictions[x[1]] == -1).map(lambda x: x[0])

# Convert anomalies back to DataFrame and save to CSV
anomalies_df = spark.createDataFrame(anomalies, df_spark.schema)
anomalies_df.toPandas().to_csv('/path/to/save/anomalies.csv', index=False)


'''
# PANDAS 
# get specific data

cursor = collection.find({}, {
    'Time': 1, 'AccelerationX': 1, 'AccelerationY': 1, 'AccelerationZ': 1,
    'GyroscopeX': 1, 'GyroscopeY': 1, 'GyroscopeZ': 1,
    'MagnetometerX': 1, 'MagnetometerY': 1, 'MagnetometerZ': 1,
    'OrientationW': 1, 'OrientationX': 1, 'OrientationY': 1, 'OrientationZ': 1,
    'GravityX': 1, 'GravityY': 1, 'GravityZ': 1,
    'LinearAccelerationX': 1, 'LinearAccelerationY': 1, 'LinearAccelerationZ': 1,
    'Pressure': 1, 'date': 1
}).limit(10000)

# put data in pandas dataframe
df = pd.DataFrame(list(cursor))

temp = df[features]

# Normalize data
scaler = StandardScaler()
X_normalized = scaler.fit_transform(temp)

# Train Isolation Forest
model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
model.fit(X_normalized)

# Get anomaly predictions
anomaly_scores = model.decision_function(X_normalized)
predictions = model.predict(X_normalized)

# Visualize anomaly scores
plt.hist(anomaly_scores, bins=50)
plt.title("Anomaly Scores")
plt.xlabel("Anomaly Score") 
plt.ylabel("Frequency") 
plt.show()

# Filter anomalies
anomalies = df[predictions == -1]

#print(anomalies)
anomalies.to_csv('/Users/sierrapine/Documents/GitHub/Sussex-Huawei-Locomotion-Dataset-Project/scripts/anomalies.csv', index=False)
'''

'''
# Get one document
document = collection.find_one()
print(document.keys())
'''

''' KMEANS 
# Train KMeans model
kmeans = KMeans(k=2, seed=1, featuresCol="features", predictionCol="prediction")
model = kmeans.fit(vectorized_df)
predictions = model.transform(vectorized_df)

# Show anomalies (outliers can be detected by clustering)
anomalies_df = predictions.filter(predictions.prediction == 1)  # Adjust based on your cluster labels
anomalies_df.show()
'''