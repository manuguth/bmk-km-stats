# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Additional custom silver tables 

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
env = dbutils.widgets.get("env")
catalog = f"bmk_{env}"
prod_catalog = "bmk_prod"

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


