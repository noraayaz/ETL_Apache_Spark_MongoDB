from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")

db = client["yellow_taxi_db"]

stats = db.command("collstats", "processed_trips")
print(f"Storage size: {stats['storageSize'] / (1024 ** 3):.2f} GB") 
