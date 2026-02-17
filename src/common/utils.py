# Databricks notebook source
import json

# COMMAND ----------

config_path = "../../config/Pharmacy_claims.json"

# COMMAND ----------

def read_json():
    with open(f"{config_path}", "r") as f:
        config = json.load(f)
    return config

# COMMAND ----------

