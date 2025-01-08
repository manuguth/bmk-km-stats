# Databricks notebook source
# MAGIC %md
# MAGIC # Test new Bronze table

# COMMAND ----------

default_env_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
catalog = default_env_catalog
prod_catalog = "bmk_prod"
env = catalog.split("_")[-1]

# COMMAND ----------

dbutils.widgets.text("catalog", catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   aptmt.id,
# MAGIC   aptmt.typid,
# MAGIC   aptmt.name AS appointment_name,
# MAGIC   DATE(aptmt.start) AS appointment_date,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END
# MAGIC   ) AS attending,
# MAGIC   COUNT(DISTINCT matrix.kmuserid) AS invited
# MAGIC FROM
# MAGIC   IDENTIFIER(:catalog || '.bronze.km_appointments') AS aptmt
# MAGIC   LEFT JOIN IDENTIFIER(:catalog || '.bronze.km_matrix') AS matrix ON matrix.appointmentid = aptmt.id
# MAGIC WHERE
# MAGIC   DATE(aptmt.start) >= '2023-01-01'
# MAGIC   AND DATE(aptmt.start) < '2024-01-01'
# MAGIC   AND aptmt.typid IN (1, 2)
# MAGIC GROUP BY
# MAGIC   aptmt.id,
# MAGIC   aptmt.typid,
# MAGIC   aptmt.name,
# MAGIC   aptmt.start
# MAGIC ORDER BY
# MAGIC   attending DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT map.kmuserid,
# MAGIC        users.name,
# MAGIC        map.orgId,
# MAGIC        org.name,
# MAGIC        users.mail
# MAGIC FROM IDENTIFIER(:catalog || '.bronze.km_orgusermapping') AS map
# MAGIC LEFT JOIN IDENTIFIER(:catalog || '.bronze.km_kmusers') AS users ON users.id = map.kmuserid
# MAGIC LEFT JOIN IDENTIFIER(:catalog || '.bronze.km_orgs') AS org ON map.orgId = org.id
# MAGIC WHERE org.parentid IS NOT NULL
# MAGIC ORDER BY users.name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM IDENTIFIER(:catalog || '.bronze.km_kmusers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   aptmt.id,
# MAGIC   aptmt.name AS appointment_name,
# MAGIC   aptmt.start AS appointment_start,
# MAGIC   aptmt.typId as appointment_type,
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END
# MAGIC   ) AS attending
# MAGIC FROM
# MAGIC   IDENTIFIER(:catalog || '.bronze.km_appointments') AS aptmt
# MAGIC   LEFT JOIN IDENTIFIER(:catalog || '.bronze.km_matrix') AS matrix ON matrix.appointmentid = aptmt.id
# MAGIC GROUP BY
# MAGIC   aptmt.id,
# MAGIC   aptmt.name,
# MAGIC   aptmt.start,
# MAGIC   aptmt.typId
# MAGIC ORDER BY
# MAGIC   aptmt.start DESC

# COMMAND ----------


