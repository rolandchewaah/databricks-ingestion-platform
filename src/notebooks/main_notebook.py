# Databricks notebook source
# MAGIC %md
# MAGIC # Dev Environment: Main Processing
# MAGIC This notebook was automatically deployed from GitHub.

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion Pipeline: Bronze Layer
# MAGIC This notebook ingests raw data into the `ingestion_db` schema.

# 1. Setup Configuration
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

database_name = f"ingestion_platform_{env}"
table_name = "raw_traffic_data"
checkpoint_path = f"/tmp/checkpoints/{table_name}"

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")



# COMMAND ----------

# 2. Create Sample Data (For demonstration)
# In a real scenario, this would be your S3/Azure Blob path
import json

sample_data = [
    {"sensor_id": 1, "temperature": 25.2, "timestamp": "2024-01-01 10:00:00"},
    {"sensor_id": 2, "temperature": 24.8, "timestamp": "2024-01-01 10:01:00"}
]

# Generate a dummy file to ingest
raw_data_path = "/tmp/sample_raw_data/"
dbutils.fs.put(f"{raw_data_path}data.json", json.dumps(sample_data), overwrite=True)

# COMMAND ----------

# 3. Ingest Data using Auto Loader (Cloud Files)
# Auto Loader automatically handles schema inference and evolution
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
      .load(raw_data_path))

# COMMAND ----------

# 4. Write to Delta Table
# This will create the table automatically if it doesn't exist
query = (df.writeStream
         .format("delta")
         .option("checkpointLocation", checkpoint_path)
         .outputMode("append")
         .trigger(availableNow=True) # "availableNow" is perfect for CI/CD batch testing
         .toTable(table_name))

query.awaitTermination()



# COMMAND ----------

# 5. Verify the results
display(spark.sql(f"SELECT * FROM {table_name}"))
