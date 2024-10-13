import os
import pandas as pd
import time
from pymongo import MongoClient
import config

start_time = time.time()

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.get_database("Project")

# Directory containing your folders and CSV files
base_directory = config.base_directory

for folder_name in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder_name)

    if os.path.isdir(folder_path):  # Check if it is a directory
        for csv_file in os.listdir(folder_path):
            if csv_file.endswith('.csv'):  # Process only CSV files
                file_path = os.path.join(folder_path, csv_file)

                try:
                    # Read the CSV file into a DataFrame
                    df = pd.read_csv(file_path)

                    # Add the folder name as a new column
                    # Adjust the column name as needed
                    df['date'] = folder_name

                    # Insert DataFrame into MongoDB
                    file_name_without_extension = os.path.splitext(
                        os.path.basename(csv_file))[0]
                    # Use a static collection name for testing
                    collection_name = file_name_without_extension
                    db[collection_name].insert_many(df.to_dict('records'))

                    print(
                        f'Uploaded {csv_file} to collection {collection_name} with folder name {folder_name}')

                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

print("Data upload complete.")
end_time = time.time()
print(f'Time taken to ingest data: {end_time - start_time} seconds')
