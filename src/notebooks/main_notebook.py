# Databricks notebook source
# MAGIC %md
# MAGIC # Dev Environment: Main Processing
# MAGIC This notebook was automatically deployed from GitHub.

# COMMAND ----------

# Import custom logic from our modules folder
import sys
import os
sys.path.append(os.path.abspath('../modules'))
from data_utils import get_greeting

# COMMAND ----------

# Define a widget for parameters
dbutils.widgets.text("environment", "dev")
env = dbutils.widgets.get("environment")

# Use our imported function
print(f"{get_greeting('Engineer')}!")
print(f"Current Environment: {env}")

# COMMAND ----------

# Sample Spark Logic
df = spark.range(1, 11).withColumn("is_even", (spark.col("id") % 2 == 0))
display(df)