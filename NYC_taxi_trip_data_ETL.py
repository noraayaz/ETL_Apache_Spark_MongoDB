import os
import json
import requests
from time import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, LongType, StringType, TimestampType
from pymongo import MongoClient
import pyarrow.parquet as pq
import pyarrow as pa

# Initialize Spark session
spark = SparkSession.builder \
    .master("spark://localhost:7077") \
    .appName("Yellow Taxi ETL Pipeline") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .config("spark.cores.max", "8") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

# Base URL and file patterns
base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
file_pattern = "yellow_tripdata_{year}-{month:02d}.parquet"

# Directories for files
local_folder = "/usr/local/spark/data/yellow_taxi/"
processed_folder = "/usr/local/spark/data/yellow_taxi_fixed/"
metadata_downloaded = os.path.join(local_folder, "downloaded_files.json")
metadata_processed = os.path.join(processed_folder, "processed_files.json")

# Create directories if they don't exist
os.makedirs(local_folder, exist_ok=True)
os.makedirs(processed_folder, exist_ok=True)

# List of years to process
years = [2023, 2024]

# Helper functions for metadata management
def load_metadata(path):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def save_metadata(path, data):
    with open(path, "w") as f:
        json.dump(data, f)

# Step 1: Download missing files
def download_new_files():
    """
    Download Yellow Taxi data files that are not already downloaded.
    """
    downloaded_files = load_metadata(metadata_downloaded)

    for year in years:
        for month in range(1, 13):
            file_name = file_pattern.format(year=year, month=month)
            file_url = base_url + file_name
            local_file_path = os.path.join(local_folder, file_name)

            if file_name in downloaded_files or os.path.exists(local_file_path):
                print(f"File already exists, skipping: {file_name}")
                continue

            try:
                print(f"Downloading {file_url}...")
                response = requests.get(file_url)
                response.raise_for_status()
                with open(local_file_path, "wb") as f:
                    f.write(response.content)
                print(f"Successfully downloaded: {file_name}")
                downloaded_files.append(file_name)
                save_metadata(metadata_downloaded, downloaded_files)
            except requests.exceptions.RequestException as e:
                print(f"Failed to download {file_name}: {e}")

# Step 2: Process and transform files
def process_files():
    """
    Process downloaded files and save transformed data to MongoDB.
    """
    downloaded_files = load_metadata(metadata_downloaded)
    processed_files = load_metadata(metadata_processed)
    to_process_files = [file for file in downloaded_files if file not in processed_files]

    if not to_process_files:
        print("No new files to process.")
        return

    print(f"Processing files: {to_process_files}")

    # Define schema for Spark
    schema_spark = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", LongType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", LongType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
        StructField("Airport_fee", DoubleType(), True)
    ])

    for file_name in to_process_files:
        try:
            original_file = os.path.join(local_folder, file_name)
            converted_file = os.path.join(processed_folder, file_name)

            print(f"Processing file: {original_file}")

            # Start timer
            start_time = time()

            # Read transformed file into Spark
            spark_df = spark.read.schema(schema_spark).parquet(original_file)

            read_time = time() - start_time
            print(f"Read file in {read_time:.2f} seconds.")

            # Transform data in Spark
            start_time = time()
            spark_df = spark_df.filter((col("trip_distance") > 0) & (col("total_amount") > 0))
            spark_df = spark_df.filter(unix_timestamp("tpep_dropoff_datetime") > unix_timestamp("tpep_pickup_datetime"))
            spark_df = spark_df.select(
                "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                "trip_distance", "PULocationID", "DOLocationID", "tip_amount", "total_amount"
            ).withColumn(
                "trip_duration_minutes",
                (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60
            ).withColumn(
                "tip_ratio", col("tip_amount") / col("total_amount")
            ).withColumn(
                "distance_segment",
                when(col("trip_distance") <= 2, "short")
                .when(col("trip_distance") <= 10, "medium")
                .otherwise("long")
            )

            transform_time = time() - start_time
            print(f"Transformations completed in {transform_time:.2f} seconds.")

            # Save transformed data to MongoDB
            start_time = time()
            save_to_mongodb(spark_df)

            mongo_time = time() - start_time
            print(f"Saved to MongoDB in {mongo_time:.2f} seconds.")

            # Mark file as processed
            processed_files.append(file_name)
            save_metadata(metadata_processed, processed_files)
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

# Step 3: Save data to MongoDB
def save_to_mongodb(df):
    """
    Save the processed DataFrame to MongoDB.
    """
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["yellow_taxi_db"]
        collection = db["processed_trips"]

        pandas_df = df.toPandas()
        collection.insert_many(pandas_df.to_dict("records"))
        print("Data successfully loaded into MongoDB!")
    except Exception as e:
        print(f"Error during MongoDB insertion: {e}")

# Execute ETL pipeline
download_new_files()
process_files()
