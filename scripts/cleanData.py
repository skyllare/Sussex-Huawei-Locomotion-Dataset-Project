import pymongo
from pymongo import MongoClient
import numpy as np

MONGO_URI = 'mongodb://localhost:27017/'
DATABASE_NAME = 'Project'
IGNORED_COLUMNS_GENERAL = ['Ignore1', 'Ignore2']
IGNORED_COLUMNS_MOTION = ['Altitude', 'Temperature']

# Checks if value is NaN
def is_nan(value):
    return isinstance(value, float) and np.isnan(value)

# Removes documents containing NaN or null values, and delete ignored columns
def clean_collection(collection, ignored_columns):
    print(f"Cleaning collection: {collection.name}")
    
    # Fetch all documents in the collection
    cursor = collection.find()
    to_delete = []
    bulk_operations = []

    for doc in cursor:
        doc_id = doc['_id']
        has_nan_or_null = False
        update_needed = False

        # Remove ignored columns from the document
        for column in ignored_columns:
            if column in doc:
                del doc[column]
                update_needed = True  # Mark that the document needs updating

        # Check remaining fields for NaN or null
        for key, value in doc.items():
            if value is None or is_nan(value):
                has_nan_or_null = True
                break
        
        if has_nan_or_null:
            # Collect document IDs for deletion
            to_delete.append(doc_id)
        elif update_needed:
            # Update document if columns were removed
            bulk_operations.append(
                pymongo.UpdateOne({"_id": doc_id}, {"$set": doc})
            )
    
    # Perform bulk delete of documents with NaN/null values
    if to_delete:
        result = collection.delete_many({'_id': {'$in': to_delete}})
        print(f"Deleted {result.deleted_count} documents from {collection.name}")
    
    # Perform bulk update to remove ignored columns
    if bulk_operations:
        result = collection.bulk_write(bulk_operations)
        print(f"Updated {result.modified_count} documents in {collection.name} (removed ignored columns)")

def main():
    # Connect to MongoDB
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    
    # List of collections to clean
    collections = db.list_collection_names()

    for collection_name in collections:
        collection = db[collection_name]
        
        # Check for Hips_Motion collection and apply specific logic
        if collection_name == 'Hips_Motion':
            clean_collection(collection, IGNORED_COLUMNS_MOTION)
        else:
            clean_collection(collection, IGNORED_COLUMNS_GENERAL)
    
    print("Data cleaning complete.")

if __name__ == "__main__":
    main()
