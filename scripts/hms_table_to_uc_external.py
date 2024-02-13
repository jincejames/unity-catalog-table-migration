# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables outside of DBFS to UC External Tables
# MAGIC
# MAGIC This notebook will migrate external table(s) outside of DBFS in a given schema from the Hive Metastore to a UC catalog using SYNC command.
# MAGIC
# MAGIC **Important:**
# MAGIC - This notebook needs to run on a cluster with **spark.databricks.sql.initial.catalog.name set to hive_metastore** or the base catalog where the external tables will be pulled
# MAGIC - **Tables on DBFS** - this means the files reside completely within DBFS and the only way forward for these are to recreate them via CLONE
# MAGIC
# MAGIC **Migration away from Mounts points**
# MAGIC
# MAGIC There is no support for mount points with Unity Catalog. Existing mount points should be upgraded to External Locations.
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have external location(s) inplace for the table(s)' mounted file path(s) that you want to migrate.
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table`** (mandatory): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2". If filled only the given table(s) will be pulled otherwise all the tables.
# MAGIC * **`Target UC Catalog`** (mandatory):
# MAGIC   - The name of the target catalog.
# MAGIC * **`Target UC Schema`** (mandatory):
# MAGIC   - The name of the target schema.
# MAGIC * **`Dry run`** (mandatory):
# MAGIC   - If "Y" then only simulate the migration.   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set widgets

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source Schema")
dbutils.widgets.text("source_table", "", "Source Table(s)")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.dropdown("dry_run", "Y", ["Y", "N"], "Dry run")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
dry_run = True if dbutils.widgets.get("dry_run") == "Y" else False
# Variables mustn't be changed
source_catalog = "hive_metastore"
table_type = "table"

# COMMAND ----------

from utils.utils import get_table_description, sync_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed tables descriptions if the `Source Table(s)`  parameter is empty
# MAGIC - Get the given managed table(s) description if the `Source Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the Hive Metastore table to Unity Catalog
# MAGIC
# MAGIC **Note**:
# MAGIC - The **tables will be created with the same name in Unity Catalog** as in the Hive Metastore

# COMMAND ----------

# Create empty sync status list
sync_status_list = []
# Iterate through the table descriptions
for table_details in tables_descriptions:
  # Sync table
  sync_status = sync_table(spark, table_details, target_catalog, target_schema, dry_run)
  # Append sync status list
  sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)

# COMMAND ----------

