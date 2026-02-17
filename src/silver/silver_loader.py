# Databricks notebook source
# DBTITLE 1,Cell 1
import json

# COMMAND ----------

# MAGIC %run ../common/dq_engine.py

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

#Initialize hte engine
config = read_json()
engine = DataQualityEngine(config)

# COMMAND ----------

#Read the bronze table
bronze_table = engine.get_table_name("bronze")
df_bronze = spark.read.table(bronze_table)

# COMMAND ----------

#Run the engine to apply DQ check
is_valid, summary_report = engine.run_validation(df_bronze)

# COMMAND ----------

# display(summary_report)
if not is_valid:
    failure_msg = f"DQ Gate Failed for {config['source_name']}. Check the summary table above."
    raise Exception(failure_msg)

silver_table = engine.get_table_name("silver")
(df_bronze.write
    .format("delta")
    .mode("append")
    .saveAsTable(silver_table))

# COMMAND ----------

