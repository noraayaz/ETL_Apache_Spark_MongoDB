from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["yellow_taxi_db"]
collection = db["processed_trips"]

total_records = collection.count_documents({})
print(f"Total records: {total_records}")

for doc in collection.find().limit(5):
    print(doc)
