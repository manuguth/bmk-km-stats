# Databricks notebook source
# MAGIC %md
# MAGIC # Additional custom silver tables 

# COMMAND ----------

default_env_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
catalog = default_env_catalog
prod_catalog = "bmk_prod"
env = catalog.split("_")[-1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appointments code table

# COMMAND ----------

dbutils.widgets.text("catalog", catalog)

# COMMAND ----------

id_mapping = [
    {"typId": 1, "name": "Probe"},
    {"typId": 2, "name": "Auftritt"},
    {"typId": 3, "name": "Sonstiges"},
]

# COMMAND ----------

id_mapping_df = spark.createDataFrame(id_mapping)
id_mapping_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.km_appointments_codes")

# COMMAND ----------


