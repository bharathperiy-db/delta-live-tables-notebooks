# Databricks notebook source
import re

def get_current_username():
    try:
        return dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
    except Exception as e2:
        try:
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
        except Exception as e:
            print(f"WARN: couldn't get current username. This shouldn't happen - unpredictable behavior - 2 errors: {e2} - {e} - will return 'unknown'")
            return "unknown"

def get_cleaned_username():
  user_name = get_current_username()
  return re.sub('[^\w]+', '_', user_name)

#Get username
user_name = get_cleaned_username()

#Specify demo path and table variables
#demo_path = f"dbfs:/tmp/{user_name}/dlt_integrals/"
demo_path = f"dbfs:/tmp/dlt_integrals/"
# Create demo location
dbutils.fs.mkdirs(demo_path)

# Database/schema as the TARGET for DLT pipeline
catalog = "hive_metastore"
#schema_name = f"{user_name}_demo_dlt_integrals"
schema_name = f"demo_dlt_integrals"
raw_table = "raw"

print(f"--> Demo data will be configured in schema {schema_name} and materialized at location {demo_path}")

# COMMAND ----------


