from pyspark.sql.functions import lit
from pymongo import MongoClient
from datetime import datetime


# Function to upload stats to MongoDB
def upload_stats_to_mongo(dataset_stats_df, daily_stats_df):
    # Establish MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client.get_database("Project")

    # Upload dataset-wide stats
    if dataset_stats_df:
        dataset_stats_collection = db["dataset_stats"]
        dataset_stats_pd = dataset_stats_df.toPandas()
        dataset_stats_records = dataset_stats_pd.to_dict(orient="records")
        dataset_stats_collection.delete_many({})  # Clear previous records
        dataset_stats_collection.insert_many(dataset_stats_records)
        print("Dataset-wide stats uploaded successfully!")

    # Upload daily stats
    if daily_stats_df:
        daily_stats_collection = db["daily_stats"]
        daily_stats_pd = daily_stats_df.toPandas()
        daily_stats_records = daily_stats_pd.to_dict(orient="records")
        daily_stats_collection.delete_many({})  # Clear previous records
        daily_stats_collection.insert_many(daily_stats_records)
        print("Daily statistics stats successfully!")
