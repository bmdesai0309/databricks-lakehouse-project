# Databricks notebook source
import json
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
from datetime import datetime

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

config = read_json()
raw_path = config["storage"]["raw_path"]
bronze_table = f"{config['unity_catalog']['catalog']}.{config['unity_catalog']['schema']}.{config['unity_catalog']['bronze_table']}"
partitions = config["storage"]["partition_columns"]

# COMMAND ----------

# DBTITLE 1,Cell 5
try:
    raw_df = spark.read.format(config["data_format"]).load(raw_path)
    bronze_df = (raw_df
        .withColumn("_input_file_name", F.col("_metadata.file_path"))
        .withColumn("_load_timestamp", F.current_timestamp())
        .withColumn("_batch_id", F.lit(datetime.now().strftime('%Y%m%d%H%M')))
    )
    
    (bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy(*partitions)
        .option("mergeSchema", "true")
        .saveAsTable(bronze_table)
    )
except AnalysisException:
    print("File not found")
    raise

# COMMAND ----------

