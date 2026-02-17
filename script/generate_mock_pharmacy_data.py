# Databricks notebook source
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os

# COMMAND ----------

#Initialize Faker for mock data generation
fake = Faker()

# COMMAND ----------

def generate_pharmacy_data(num_records=1000, start_date='2025-01-01', end_date='2025-02-15'):
    data = []
    
    # Define some common drug names for realism
    drugs = ["Lisinopril", "Levothyroxine", "Atorvastatin", "Metformin", "Amlodipine", "Albuterol"]
    
    for _ in range(num_records):
        # Generate random date
        fill_date = fake.date_between(start_date=datetime.strptime(start_date, '%Y-%m-%d'), 
                                      end_date=datetime.strptime(end_date, '%Y-%m-%d'))
        
        record = {
            "prescription_id": f"RX-{fake.unique.random_number(digits=8)}",
            "patient_id": f"PAT-{fake.random_number(digits=6)}",
            "drug_name": np.random.choice(drugs),
            "dosage_mg": np.random.choice([5, 10, 20, 40, 50]),
            "quantity": np.random.randint(7, 90),
            "unit_price": round(np.random.uniform(10.0, 150.0), 2),
            "pharmacy_npi": fake.random_number(digits=10),
            "fill_date": fill_date,
            "year": fill_date.year,
            "month": fill_date.month,
            "day": fill_date.day
        }
        
        # INTENTIONAL DATA QUALITY ISSUES (for framework testing)
        # 5% chance of a null prescription_id to test your "Pass 100%" rule
        if np.random.random() < 0.05:
            record["prescription_id"] = None
            
        data.append(record)
    
    return pd.DataFrame(data)

# COMMAND ----------

# DBTITLE 1,Cell 5
NUM_ROWS = 50000
RAW_PATH = "/Volumes/workspace/default/pharmacy_data/raw/"

# COMMAND ----------

# DBTITLE 1,Cell 6
data = generate_pharmacy_data(NUM_ROWS)
spark_df = spark.createDataFrame(data)

#Write Pharmacy Data as Parquet format Partitioned by Year, Month, Day
(spark_df
 .write
 .mode("overwrite")
 .partitionBy("year", "month", "day")
 .parquet(RAW_PATH)
)

# COMMAND ----------

