# Databricks notebook source
# MAGIC %md
# MAGIC # Creating Gold Tables for BMK Konzertmeister statistics

# COMMAND ----------

default_env_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
catalog = default_env_catalog
prod_catalog = "bmk_prod"
env = catalog.split("_")[-1]

# COMMAND ----------

dbutils.widgets.text("catalog", catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appointments statistics per event

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.V_km_appointments_summary AS
# MAGIC WITH appointment_data AS (
# MAGIC   SELECT
# MAGIC     aptmt.id,
# MAGIC     aptmt.typid,
# MAGIC     codes.name as aptmt_type,
# MAGIC     aptmt.name AS appointment_name,
# MAGIC     DATE(aptmt.start) AS appointment_date,
# MAGIC     COUNT(DISTINCT matrix.kmuserid) AS invited,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.negative = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS negative,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.positive = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS positive,
# MAGIC       SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.maybe = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS maybe,
# MAGIC         SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS attending
# MAGIC   FROM
# MAGIC     silver.km_appointments AS aptmt
# MAGIC     LEFT JOIN silver.km_matrix AS matrix ON matrix.appointmentid = aptmt.id
# MAGIC     left join silver.km_appointments_codes as codes on codes.typid = aptmt.typid
# MAGIC   WHERE
# MAGIC     1 = 1 -- AND aptmt.typid IN (1, 2)
# MAGIC   GROUP BY
# MAGIC     aptmt.id,
# MAGIC     aptmt.typid,
# MAGIC     aptmt.name,
# MAGIC     aptmt.start,
# MAGIC     codes.name
# MAGIC )
# MAGIC SELECT
# MAGIC   aptmt.*,
# MAGIC   ROUND(aptmt.attending / aptmt.invited, 2) as attendance_rate,
# MAGIC   ROUND((aptmt.positive + aptmt.maybe + aptmt.negative) / aptmt.invited, 2) as response_rate
# MAGIC FROM
# MAGIC   appointment_data as aptmt
# MAGIC where
# MAGIC   1 = 1
# MAGIC   -- and attending = 0
# MAGIC -- order BY
# MAGIC   -- aptmt.appointment_date desc
# MAGIC   ;

# COMMAND ----------

# query = f"""
# CREATE VIEW {catalog}.gold.V_km_appointments_summary AS
# WITH appointment_data AS (
#   SELECT
#     aptmt.id,
#     aptmt.typid,
#     codes.name as aptmt_type,
#     aptmt.name AS appointment_name,
#     DATE(aptmt.start) AS appointment_date,
#     COUNT(DISTINCT matrix.kmuserid) AS invited,
#     SUM(
#       CASE
#         WHEN matrix.negative = 'True' THEN 1
#         ELSE 0
#       END
#     ) AS negative,
#     SUM(
#       CASE
#         WHEN matrix.positive = 'True' THEN 1
#         ELSE 0
#       END
#     ) AS positive,
#       SUM(
#       CASE
#         WHEN matrix.maybe = 'True' THEN 1
#         ELSE 0
#       END
#     ) AS maybe,
#         SUM(
#       CASE
#         WHEN matrix.attendingreal = 'True' THEN 1
#         ELSE 0
#       END
#     ) AS attending
#   FROM
#     {catalog}.silver.km_appointments AS aptmt
#     LEFT JOIN {catalog}.silver.km_matrix AS matrix ON matrix.appointmentid = aptmt.id
#     LEFT JOIN {catalog}.silver.km_appointments_codes AS codes ON codes.typid = aptmt.typid
#   WHERE
#     1 = 1 -- AND aptmt.typid IN (1, 2)
#   GROUP BY
#     aptmt.id,
#     aptmt.typid,
#     aptmt.name,
#     aptmt.start,
#     codes.name
# )
# SELECT
#   aptmt.*,
#   ROUND(aptmt.attending / aptmt.invited, 2) as attendance_rate
# FROM
#   appointment_data as aptmt
# WHERE
#   1 = 1
#   -- and attending = 0
# -- order BY
#   -- aptmt.appointment_date desc
# ;
# """

# # Execute the query
# spark.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IDENTIFIER(:catalog || '.gold.V_km_appointments_summary')
# MAGIC order by appointment_date desc

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## KM user statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE VIEW gold.V_km_user_attendance AS --
# MAGIC WITH user_attendance AS (
# MAGIC   SELECT
# MAGIC     map.kmuserid,
# MAGIC     users.name as user_name,
# MAGIC     map.orgId,
# MAGIC     org.name as org_name,
# MAGIC     matrix.appointmentid,
# MAGIC     aptmt.start as appointment_date,
# MAGIC     aptmt.typid as aptmt_type_id,
# MAGIC     codes.name as aptmt_type_name,
# MAGIC     CASE
# MAGIC       WHEN matrix.negative = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS negative,
# MAGIC     CASE
# MAGIC       WHEN matrix.positive = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS positive,
# MAGIC     CASE
# MAGIC       WHEN matrix.maybe = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS maybe,
# MAGIC     CASE
# MAGIC       WHEN matrix.maybe = 'True' THEN 1
# MAGIC       WHEN matrix.negative = 'True' THEN 1
# MAGIC       WHEN matrix.positive = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS responded,
# MAGIC     CASE
# MAGIC       WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC       ELSE 0
# MAGIC     END AS attending
# MAGIC   FROM
# MAGIC     silver.km_orgusermapping AS map
# MAGIC     LEFT JOIN silver.km_kmusers AS users ON users.id = map.kmuserid
# MAGIC     LEFT JOIN silver.km_orgs AS org ON map.orgId = org.id
# MAGIC     LEFT JOIN silver.km_matrix AS matrix ON matrix.kmUserId = map.kmuserid
# MAGIC     LEFT JOIN silver.km_appointments AS aptmt ON aptmt.id = matrix.appointmentid
# MAGIC     LEFT JOIN silver.km_appointments_codes as codes on codes.typid = aptmt.typid
# MAGIC   WHERE
# MAGIC     org.parentid IS NOT NULL
# MAGIC     AND map.active = 1
# MAGIC )
# MAGIC select
# MAGIC   usratt.*
# MAGIC from
# MAGIC   user_attendance as usratt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.V_km_user_attendance_summary_eom AS
# MAGIC WITH user_attendance AS (
# MAGIC   SELECT
# MAGIC     map.kmuserid,
# MAGIC     users.name as user_name,
# MAGIC     map.orgId,
# MAGIC     org.name as org_name,
# MAGIC     COUNT(DISTINCT matrix.appointmentid) AS invited,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.negative = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS negative,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.positive = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS positive,
# MAGIC       SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.maybe = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS maybe,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.maybe = 'True' THEN 1
# MAGIC         WHEN matrix.negative = 'True' THEN 1
# MAGIC         WHEN matrix.positive = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS responded,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS attending,
# MAGIC     year(aptmt.start) as year,
# MAGIC     month(aptmt.start) as month
# MAGIC   FROM
# MAGIC     silver.km_orgusermapping AS map
# MAGIC     LEFT JOIN silver.km_kmusers AS users ON users.id = map.kmuserid
# MAGIC     LEFT JOIN silver.km_orgs AS org ON map.orgId = org.id
# MAGIC     LEFT JOIN silver.km_matrix AS matrix ON matrix.kmUserId = map.kmuserid
# MAGIC     LEFT JOIN silver.km_appointments AS aptmt ON aptmt.id = matrix.appointmentid
# MAGIC     LEFT JOIN silver.km_appointments_codes as codes on codes.typid = aptmt.typid
# MAGIC   WHERE
# MAGIC     org.parentid IS NOT NULL
# MAGIC     AND map.active = 1
# MAGIC   GROUP BY
# MAGIC     map.kmuserid,
# MAGIC     users.name,
# MAGIC     map.orgId,
# MAGIC     org.name,
# MAGIC     year(aptmt.start),
# MAGIC     month(aptmt.start)
# MAGIC )
# MAGIC select
# MAGIC   usratt.*,
# MAGIC   ROUND(usratt.responded / usratt.invited, 2) as response_rate,
# MAGIC   ROUND(usratt.attending / usratt.invited, 2) as attendance_rate
# MAGIC from
# MAGIC   user_attendance as usratt 
# MAGIC   -- ORDER BY
# MAGIC   -- usratt.user_name,
# MAGIC   -- usratt.year,
# MAGIC   -- usratt.month
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.V_km_user_attendance_summary_eoy AS
# MAGIC WITH user_attendance AS (
# MAGIC   SELECT
# MAGIC     map.kmuserid,
# MAGIC     users.name as user_name,
# MAGIC     map.orgId,
# MAGIC     org.name as org_name,
# MAGIC     COUNT(DISTINCT matrix.appointmentid) AS invited,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.negative = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS negative,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.positive = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS positive,
# MAGIC       SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.maybe = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS maybe,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.maybe = 'True' THEN 1
# MAGIC         WHEN matrix.negative = 'True' THEN 1
# MAGIC         WHEN matrix.positive = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS responded,
# MAGIC     SUM(
# MAGIC       CASE
# MAGIC         WHEN matrix.attendingreal = 'True' THEN 1
# MAGIC         ELSE 0
# MAGIC       END
# MAGIC     ) AS attending,
# MAGIC     year(aptmt.start) as year
# MAGIC   FROM
# MAGIC     silver.km_orgusermapping AS map
# MAGIC     LEFT JOIN silver.km_kmusers AS users ON users.id = map.kmuserid
# MAGIC     LEFT JOIN silver.km_orgs AS org ON map.orgId = org.id
# MAGIC     LEFT JOIN silver.km_matrix AS matrix ON matrix.kmUserId = map.kmuserid
# MAGIC     LEFT JOIN silver.km_appointments AS aptmt ON aptmt.id = matrix.appointmentid
# MAGIC     LEFT JOIN silver.km_appointments_codes as codes on codes.typid = aptmt.typid
# MAGIC   WHERE
# MAGIC     org.parentid IS NOT NULL
# MAGIC     AND map.active = 1
# MAGIC   GROUP BY
# MAGIC     map.kmuserid,
# MAGIC     users.name,
# MAGIC     map.orgId,
# MAGIC     org.name,
# MAGIC     year(aptmt.start)
# MAGIC )
# MAGIC select
# MAGIC   usratt.*,
# MAGIC   ROUND(usratt.responded / usratt.invited, 2) as response_rate,
# MAGIC   ROUND(usratt.attending / usratt.invited, 2) as attendance_rate
# MAGIC from
# MAGIC   user_attendance as usratt 

# COMMAND ----------

# MAGIC %md
# MAGIC ## attendance User ranking

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   gold.V_km_user_attendance_summary_eoy
# MAGIC where
# MAGIC   year = 2024
# MAGIC order by attendance_rate desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from 
# MAGIC gold.V_km_user_attendance

# COMMAND ----------


