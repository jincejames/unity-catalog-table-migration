# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Managed Tables to Hive Metastore External Tables using DEEP CLONE
# MAGIC
# MAGIC This notebook will migrate managed hive metastore tables to external hive metastore tables, then replace the managed table with an external one.
# MAGIC
# MAGIC **Cloning:**
# MAGIC
# MAGIC You can create a copy of an existing Delta Lake table on AWS at a specific version using the clone command. Clones can be either deep or shallow. For migrating managed HMS tables to HMS external tables, we are using *deep clone*.
# MAGIC
# MAGIC A *deep clone* is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. Additionally, stream metadata is also cloned such that a stream that writes to the Delta table can be stopped on a source table and continued on the target of a clone from where it left off.
# MAGIC
# MAGIC The metadata that is cloned includes: schema, partitioning information, invariants, nullability. For deep clones only, stream and COPY INTO metadata are also cloned. Metadata not cloned are the table description and user-defined commit metadata.
# MAGIC
# MAGIC **Note:**
# MAGIC - This notebook will delete/replace your original table.
# MAGIC - You need to pause all jobs which are using this table before running this notebook. 
# MAGIC
# MAGIC **Before you start the migration**, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - If you create External table you need to have:
# MAGIC   - You have external location(s) inplace for the table(s)' mounted file path(s) that you want to migrate.
# MAGIC   - You have `CREATE EXTERNAL TABLE` privileges on the external location(s)
# MAGIC - You have the right privileges on the target UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table(s)`** (optional): 
# MAGIC   - The name(s) of the source HMS table(s). Multiple tables should be given as follows "table_1, table_2".
# MAGIC * **`Target temporary HMS Schema`** (mandatory):
# MAGIC   - The name of the target schema where the temporary table will be generated.
# MAGIC * **`Target HMS Table Location`** (mandatory):
# MAGIC   - The location of the external table.
# MAGIC * **`Temporary HMS Table Suffix`** (optional):
# MAGIC   - The notebook will create temporary table(s) with this suffix.
# MAGIC

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source HMS Schema")
dbutils.widgets.text("source_table", "", "Source HMS Table(s)")
dbutils.widgets.text("target_schema", "", "Target temporary HMS Schema")
dbutils.widgets.text("target_table_location", "", "Target HMS Table Location")
dbutils.widgets.text("temporary_table_suffix", "temporary", "Temporary HMS Table Suffix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_schema = dbutils.widgets.get("target_schema")
target_table_location = dbutils.widgets.get("target_table_location")
temporary_table_suffix = dbutils.widgets.get("temporary_table_suffix")
# Variables mustn't be changed
source_catalog = "hive_metastore"
target_catalog = "hive_metastore"
table_type = "table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.utils import get_table_description, replace_hms_managed_table_to_hms_external

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore table(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all managed or external tables descriptions if the `Source Table(s)`  parameter is empty
# MAGIC - Get the given managed or external table(s) description if the `Source Table(s)` is filled

# COMMAND ----------

tables_descriptions = get_table_description(spark, source_catalog, source_schema, source_table, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migrating hive_metastore Managed Tables to hive_metastore External Tables with data movement using the DEEP CLONE command

# COMMAND ----------

# Set empty sync status list
sync_status_list = []
# Iterate through table descriptions
for table_details in tables_descriptions:
    # Clone
    sync_status = replace_hms_managed_table_to_hms_external(spark, dbutils, table_details, target_catalog, target_schema, temporary_table_suffix, target_table_location)
    # Append sync status list
    sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
    # If status code FAILED, exit notebook
    if sync_status.sync_status_code == "FAILED":
      dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)

# COMMAND ----------

