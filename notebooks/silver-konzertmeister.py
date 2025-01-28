# Databricks notebook source
# MAGIC %md
# MAGIC # Merging Bronze Tables into Silver Tables

# COMMAND ----------

default_env_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
catalog = default_env_catalog
prod_catalog = "bmk_prod"
env = catalog.split("_")[-1]

# COMMAND ----------

display(env)

# COMMAND ----------

print(catalog)

# COMMAND ----------

def merge_bronze_to_silver_id_tables(table_name, catalog):
    table_bronze = f"{catalog}.bronze.{table_name}"
    table_silver = f"{catalog}.silver.{table_name}"

    cols_bronze = spark.table(table_bronze).columns

    update_stmt = ""
    for col in cols_bronze:
        update_stmt += f"target.{col} = source.{col}, "
    insert_stmt = ",".join(cols_bronze)

    insert_stmt_values = ""
    for col in cols_bronze:
        insert_stmt_values += f"source.{col}, "

    merge_stmt = f"""
    MERGE INTO {table_silver} AS target
    USING {table_bronze} AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET
            {update_stmt}
            target.updated_date = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_stmt},
            created_date,
            updated_date
        ) VALUES (
            {insert_stmt_values}
            current_timestamp(),
            current_timestamp()
        )
    """

    merge_stmt += (
        "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET target.active = False"
        if "appointments" in table_name
        else ""
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_silver}
        USING DELTA
        AS SELECT *, current_timestamp() AS created_date, current_timestamp() AS updated_date FROM {table_bronze} WHERE 1=0
        """
    )
    spark.sql(merge_stmt)

# COMMAND ----------

tables = [
    "appointments",
    "orgs",
    "matrix",
    "kmusers",
    "members",
    # {"table_name": "kmuserinvitedorgs", "mrg_ids": ["kmUserId", "orgId", "appointmentId"]},
    # {"table_name": "orgusermapping", "mrg_ids": ["orgId", "kmUserId"]},
]


# COMMAND ----------

for tbl in tables:
    merge_bronze_to_silver_id_tables(
        f"km_{tbl}",
        catalog,
    )

# COMMAND ----------

def merge_bronze_to_silver_no_id_tables(
    table_name,
    mrg_ids,
    catalog,
):
    table_bronze = f"{catalog}.bronze.{table_name}"
    table_silver = f"{catalog}.silver.{table_name}"

    cols_bronze = spark.table(table_bronze).columns

    update_stmt = ""
    for col in cols_bronze:
        update_stmt += f"target.{col} = source.{col}, "
    insert_stmt = ",".join(cols_bronze)

    insert_stmt_values = ""
    for col in cols_bronze:
        insert_stmt_values += f"source.{col}, "

    merge_stmt = f"""
    MERGE INTO {table_silver} AS target
    USING {table_bronze} AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET
            {update_stmt}
            target.updated_date = current_timestamp()
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_stmt},
            created_date,
            updated_date
        ) VALUES (
            {insert_stmt_values}
            current_timestamp(),
            current_timestamp()
        )
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_silver}
        USING DELTA
        AS SELECT *, current_timestamp() AS created_date, current_timestamp() AS updated_date FROM {table_bronze} WHERE 1=0
        """
    )
    spark.sql(merge_stmt)

# COMMAND ----------

tables_no_id = [
    {"table_name": "kmuserinvitedorgs", "mrg_ids": ["kmUserId", "orgId", "appointmentId"]},
    {"table_name": "orgusermapping", "mrg_ids": ["orgId", "kmUserId"]},
    {"table_name": "roles", "mrg_ids": ["kmUserId", "role"]},
]

# COMMAND ----------

def merge_bronze_to_silver_with_active_flag(
    table_name,
    mrg_ids,
    catalog,
):
    table_bronze = f"{catalog}.bronze.{table_name}"
    table_silver = f"{catalog}.silver.{table_name}"

    cols_bronze = spark.table(table_bronze).columns

    update_stmt = ""
    insert_stmt_values = ""
    for col in cols_bronze:
        update_stmt += f"target.{col} = source.{col}, "
        insert_stmt_values += f"source.{col}, "
    insert_stmt = ",".join(cols_bronze)
    mrg_stmt = ""
    for i, col in enumerate(mrg_ids):
        if i == len(mrg_ids) - 1:
            mrg_stmt += f"target.{col} = source.{col}"
        else:
            mrg_stmt += f"target.{col} = source.{col} AND "


    merge_stmt = f"""
    MERGE INTO {table_silver} AS target
    USING {table_bronze} AS source
    ON {mrg_stmt}
    WHEN MATCHED THEN
        UPDATE SET
            {update_stmt}
            target.updated_date = current_timestamp(),
            target.active = 1
    WHEN NOT MATCHED THEN
        INSERT (
            {insert_stmt},
            created_date,
            updated_date,
            active
        ) VALUES (
            {insert_stmt_values}
            current_timestamp(),
            current_timestamp(),
            1
        )
    """

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_silver}
        USING DELTA
        AS SELECT *, current_timestamp() AS created_date, current_timestamp() AS updated_date, 0 AS active FROM {table_bronze} WHERE 1=0
        """
    )
    if spark.catalog.tableExists(table_silver):
        spark.sql(f"UPDATE {table_silver} SET active = 0")
    spark.sql(merge_stmt)

# COMMAND ----------

for tbl in tables_no_id:
    merge_bronze_to_silver_with_active_flag(
        f"km_{tbl['table_name']}",
        tbl["mrg_ids"],
        catalog,
    )

# COMMAND ----------


