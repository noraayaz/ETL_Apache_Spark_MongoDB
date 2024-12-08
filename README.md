# Automated ETL Pipeline for NYC Taxi Data Processing with Apache Spark and MongoDB

This project is part of the **Datafångst, migrering och förädling (ETL/ELT)** course at **Nackademin**, where I developed a scalable, automated, and robust ETL pipeline to process NYC taxi trip data using **Apache Spark** and **MongoDB**. The pipeline is designed to handle large datasets efficiently, perform meaningful transformations, and store the processed data for downstream analytics. This project was developed and tested in a local environment with Apache Spark and MongoDB installed locally. It demonstrates how to process and store large-scale NYC taxi trip data efficiently using local resources.

---

## 📂 Project Overview

### Objectives
- **Automate** the data extraction, transformation, and loading (ETL) process.
- **Process large datasets** using a distributed computing framework (Apache Spark).
- **Store transformed data** in a NoSQL database (MongoDB) for easy querying and analysis.
- **Ensure scalability** to handle growing datasets over time.

---

## ⚙️ Key Features

### Data Pipeline
- **Source Data**: NYC Yellow Taxi trip data (2023, 2024) in .parquet format, dynamically downloaded from the [NYC Taxi Data webpage](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The pipeline is designed to automatically fetch updated data each month as it becomes available on the webpage.
- **ETL Workflow**:
  - **Extract**: Downloads raw `.parquet` files dynamically based on years and months specified.
  - **Transform**:
    - Filters out invalid records.
    - Calculates key metrics like `trip_duration_minutes`, `tip_ratio`, and `distance_segment`.
    - Segments trips into categories: "short", "medium", and "long".
  - **Load**: Saves the processed data to a MongoDB collection named `processed_trips`.

### Automation
- A **`cron` job** was set up to automate the periodic execution of the pipeline, ensuring it processes newly available data every month without manual intervention.

### Validation
- Verified processed data with:
  - MongoDB queries for accuracy.
  - PySpark transformation logs.
  - Storage metrics (MongoDB statistics).

---

## 📈 Data Pipeline Architecture

1. **Local Apache Spark Cluster**: 
   - Configured with 1 Master and 4 Workers.
   - Workers allocated with 2 CPU cores and 4GB memory each for parallel data processing.

2. **MongoDB Storage**:
   - Stored processed records in a single collection.
   - Validated with storage statistics and sample queries.

3. **Feature Engineering**:
   - Trip distance segmentation: `short`, `medium`, `long`.
   - Tip percentage (`tip_ratio`).
   - Trip duration in minutes (`trip_duration_minutes`).

---

## 🖥️ How to Run

### Prerequisites
- Install **Apache Spark** and **MongoDB**.
- Python 3.9+ with the following libraries:
  - `pyspark`
  - `pymongo`
  - `requests`

### Local Environment Setup
- Ensure that **Apache Spark** and **MongoDB** are installed and configured on your local machine.
- The pipeline was developed and tested on a local machine with the following specifications:
  - **OS**: macOS/Linux
  - **RAM**: 16 GB
  - **CPU**: Quad-core processor

### Steps
1. **Set up Spark**:
    - Start the Spark master:
      ```bash
      ./sbin/start-master.sh
      ```
    - Start Spark workers:
      ```bash
      ./sbin/start-worker.sh spark://localhost:7077
      ```

2. **Run the ETL Pipeline**:
    ```bash
    python NYC_taxi_trip_data_ETL.py
    ```

3. **Validate Data**:
    - Access MongoDB shell:
      ```bash
      mongosh
      use yellow_taxi_db
      db.processed_trips.countDocuments()
      db.processed_trips.find().limit(5)
      ```

4. **Automate with Cron**:
    Add the following cron job to process new data monthly:
    ```bash
    crontab -e
    0 0 1 * * /path/to/NYC_taxi_trip_data_ETL.py
    ```

---

## 📊 Results

- **Performance**: Data processing and transformations completed in under **150 seconds per file** on average.
- **MongoDB Storage**: Total storage usage after inserting processed data was approximately **5.68 GB**.
- **Spark Monitoring**: Used Spark UI to track and debug the pipeline stages effectively.

---

## 📬 Contact

If you have any questions or feedback, feel free to reach out!

- **Email**: [noraayaz@outlook.com](mailto:noraayaz@outlook.com)
- **GitHub**: [github.com/noraayaz](https://github.com/noraayaz)
- **LinkedIn**: [linkedin.com/in/noraayaz89](https://linkedin.com/in/noraayaz89)

---


