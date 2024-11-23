from pymongo import MongoClient, UpdateOne
import re
from multiprocessing import Pool

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Project"]
collections = db.list_collection_names()


# Function to standardize dates to MM/DD/YYYY format
def standardize_date(date_str):
    clean_date = re.sub(r'\D', '', date_str)
    if len(clean_date) != 6:
        return None
    day, month, year = clean_date[:2], clean_date[2:4], clean_date[4:]
    year = f"20{year}" if int(year) < 50 else f"19{year}"
    return f"{month}/{day}/{year}"


# Function to process documents in a collection
def process_documents(collection_name, batch_size=1000):
    print(f"Processing collection: {collection_name}")
    collection = db[collection_name]
    cursor = collection.find({"date": {"$exists": True}}, {
                             "_id": 1, "date": 1})
    batch = []

    for document in cursor:
        original_date = document["date"]
        standardized_date = standardize_date(original_date)
        if standardized_date:
            batch.append(UpdateOne(
                {"_id": document["_id"]},
                {"$set": {"date": standardized_date}}
            ))

        # Process the batch when it reaches the specified size
        if len(batch) >= batch_size:
            bulk_update(collection, batch)
            batch.clear()

    # Process any remaining documents
    if batch:
        bulk_update(collection, batch)

    print(f"Finished processing collection: {collection_name}")


# Function to perform bulk updates
def bulk_update(collection, batch):
    collection.bulk_write(batch)


# Function to standardize dates in all collections
def standardize_dates_in_all_collections():
    collections_to_process = [col for col in collections]
    with Pool() as pool:
        pool.map(process_documents, collections_to_process)


if __name__ == "__main__":
    standardize_dates_in_all_collections()
