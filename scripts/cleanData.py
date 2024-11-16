import pymongo
from pymongo import MongoClient, UpdateOne
import numpy as np

MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'Project'
IGNORED_COLUMNS_GENERAL = ['Ignore1', 'Ignore2']
IGNORED_COLUMNS_MOTION = ['Altitude', 'Temperature']
BATCH_SIZE = 1000  # Number of documents to process in each batch

# Checks if value is NaN
def is_nan(value):
    return isinstance(value, float) and np.isnan(value)

# Removes documents containing NaN or null values, and delete ignored columns
def clean_collection(collection, ignored_columns):
    print(f"Cleaning collection: {collection.name}")
    
    cursor = collection.find()
    to_delete = []
    bulk_operations = []
    processed_count = 0

    for doc in cursor:
        doc_id = doc['_id']
        has_nan_or_null = False
        update_needed = False
        unset_fields = {}

        # Remove ignored columns from the document
        for column in ignored_columns:
            if column in doc:
                print(f"Removing column: {column} from document ID: {doc_id}")
                unset_fields[column] = ""
                update_needed = True

        # Check remaining fields for NaN or null
        for key, value in doc.items():
            if value is None or is_nan(value):
                has_nan_or_null = True
                break
        
        if has_nan_or_null:
            to_delete.append(doc_id)
        elif update_needed:
            bulk_operations.append(
                UpdateOne({"_id": doc_id}, {"$unset": unset_fields})
            )
        
        processed_count += 1

        # Perform bulk operations in batches
        if processed_count % BATCH_SIZE == 0:
            if to_delete:
                result = collection.delete_many({'_id': {'$in': to_delete}})
                to_delete = []
            
            if bulk_operations:
                result = collection.bulk_write(bulk_operations)
                bulk_operations = []

    # Perform remaining bulk operations
    if to_delete:
        result = collection.delete_many({'_id': {'$in': to_delete}})
    
    if bulk_operations:
        result = collection.bulk_write(bulk_operations)

    print(f"Finished processing {processed_count} documents in total.")

def main():
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]

    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]

        # Check for Hips_Motion collection and remove specific columns
        if collection_name == 'Hips_Motion':
            clean_collection(collection, IGNORED_COLUMNS_MOTION)
        else:
            clean_collection(collection, IGNORED_COLUMNS_GENERAL)

    print("Data cleaning complete.")


if __name__ == "__main__":
    main()
