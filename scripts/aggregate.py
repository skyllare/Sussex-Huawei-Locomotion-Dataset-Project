import pymongo
from pymongo import MongoClient
import numpy as np

MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'Project'


def main():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client.get_database(DATABASE_NAME)
    collection_label = db.get_collection("Label")
    
    # List of collections to clean
    unique_coarse_labels = collection_label.distinct("CoarseLabel")
    unique_count = len(unique_coarse_labels)
    transportation_type = unique_count == 9
    print("There is the expected number of transportation types")
    collection_motion = db.get_collection("Hips_Motion")
    unique_date_labels = collection_label.distinct("date")
    unique_date_count = len(unique_date_labels)
    date_amount = unique_date_count == 91
    print("There is the expected number of unique dates")
    

if __name__ == "__main__":
    main()