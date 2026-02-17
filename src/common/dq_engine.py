# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

class DataQualityEngine:
    def __init__(self, config):
        """
        Initializes engine with the JSON config.
        """
        self.config = config
        self.rules = config.get("data_quality_rules", [])
        self.source_name = config.get("source_name", "unknown_source")
    
    def run_validation(self, df):
        total_count = df.count()
        if total_count == 0:
            print("Source data is empty. Skipping validation.")
            return True, None
        
        print(f"Validating {total_count} records for '{self.source_name}'...")

        # We will collect results in a list of dicts to show a summary table
        dq_summary = []
        is_all_valid = True

        for rule in self.rules:
            rule_name = rule['name']
            condition = rule['expectation']

            failed_df = df.filter(f"NOT ({condition})")
            fail_count = failed_df.count()

            status = "PASS" if fail_count == 0 else "FAIL"
            if fail_count > 0:
                is_all_valid = False
            
            dq_summary.append({
                "rule_name": rule_name,
                "expectation": condition,
                "fail_count": fail_count,
                "pass_percentage": ((total_count - fail_count) / total_count) * 100,
                "status": status
            })

        summary_df = spark.createDataFrame(dq_summary)
        return is_all_valid, summary_df
    
    def get_table_name(self, layer):
        uc = self.config["unity_catalog"]
        return f"{uc['catalog']}.{uc['schema']}.{uc[f'{layer}_table']}"



# COMMAND ----------

