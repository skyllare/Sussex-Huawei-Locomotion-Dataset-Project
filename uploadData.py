import os
import pandas as pd
from pymongo import MongoClient

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client.get_database("Project")  # Replace with your database name

# Directory containing your folders and CSV files
base_directory = 'G:/School/Fall 2024/CPT_S 415/Project/Data/test'

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
                    df['date'] = folder_name  # Adjust the column name as needed

                    # Insert DataFrame into MongoDB
                    file_name_without_extension = os.path.splitext(os.path.basename(csv_file))[0]
                    collection_name = file_name_without_extension  # Use a static collection name for testing
                    db[collection_name].insert_many(df.to_dict('records'))

                    print(f'Uploaded {csv_file} to collection {collection_name} with folder name {folder_name}')
                
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")

print("Data upload complete.")
