import os
import pandas as pd
import time
from pymongo import MongoClient
from config import config
import 

start_time = time.time()

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.get_database("Project")

# Directory containing your folders and CSV files
base_directory = config.base_directory

# Define the chunk size for reading CSV files
CHUNK_SIZE = 1000

# Specify the file to process
target_csv_file = "Hips_Location.csv"

for folder_name in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder_name)

    if os.path.isdir(folder_path):  # Check if it is a directory
        file_path = os.path.join(folder_path, target_csv_file)

        if os.path.exists(file_path):  # Check if the target CSV file exists
            try:
                # Define the MongoDB collection name based on the file name
                collection_name = os.path.splitext(target_csv_file)[0]
                
                # Read the CSV in chunks and insert them into MongoDB
                for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
                    # Add the folder name as a new column
                    chunk['date'] = folder_name

                    # Insert the chunk into MongoDB
                    db[collection_name].insert_many(chunk.to_dict('records'))

                print(
                    f'Uploaded {target_csv_file} to collection {collection_name} with folder name {folder_name}')

            except Exception as e:
                print(f"Error processing {file_path}: {e}")

# Close the MongoDB connection
client.close()

end_time = time.time()
print("Data upload complete.")
print(f'Time taken to ingest data: {end_time - start_time} seconds')
