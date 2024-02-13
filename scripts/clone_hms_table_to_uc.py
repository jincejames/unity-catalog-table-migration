# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables to UC Managed or External Tables using DEEP CLONE
# MAGIC
# MAGIC This notebook will migrate managed or external table(s) from the Hive Metastore to UC catalog.
# MAGIC
# MAGIC **Cloning:**
# MAGIC
# MAGIC You can create a copy of an existing Delta Lake table at a specific version using the clone command. Clones can be either deep or shallow. For migrating HMS tables to UC managed tables, we are using *deep clone*.
# MAGIC
# MAGIC A *deep clone* is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. Additionally, stream metadata is also cloned such that a stream that writes to the Delta table can be stopped on a source table and continued on the target of a clone from where it left off.
# MAGIC
# MAGIC The metadata that is cloned includes: schema, partitioning information, invariants, nullability. For deep clones only, stream and COPY INTO metadata are also cloned. Metadata not cloned are the table description and user-defined commit metadata.
# MAGIC
# MAGIC **Note:**
# MAGIC - Deep clones do not depend on the source from which they were cloned, but are expensive to create because a deep clone copies the data as well as the metadata.
# MAGIC - Cloning with replace to a target that already has a table at that path creates a Delta log if one does not exist at that path. You can clean up any existing data by running vacuum.
# MAGIC - If an existing Delta table exists, a new commit is created that includes the new metadata and new data from the source table. This new commit is incremental, meaning that only new changes since the last clone are committed to the table.
# MAGIC - Cloning a table is not the same as Create Table As Select or CTAS. A clone copies the metadata of the source table in addition to the data. Cloning also has simpler syntax: you don’t need to specify partitioning, format, invariants, nullability and so on as they are taken from the source table.
# MAGIC - A cloned table has an independent history from its source table. Time travel queries on a cloned table will not work with the same inputs as they work on its source table.
# MAGIC
# MAGIC **Source**: https://docs.databricks.com/en/delta/clone.html
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
# MAGIC * **`Target UC Catalog`** (mandatory):
# MAGIC   - The name of the target catalog.
# MAGIC * **`Target UC Schema`** (mandatory):
# MAGIC   - The name of the target schema.
# MAGIC * **`Target UC Table Location`** (optional):
# MAGIC   - If filled, an External table will be created in the given location.
# MAGIC * **`Target UC Table`** (optional):
# MAGIC   - Only applicable if the `Source Table(s)` is filled with a **single table name**, then a name can be given for the Target UC Table. Otherwise, the `Source Table(s)` name will be used.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("source_schema", "", "Source HMS Schema")
dbutils.widgets.text("source_table", "", "Source HMS Table(s)")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")
dbutils.widgets.text("target_table_location", "", "Target UC Table Location")
dbutils.widgets.text("target_table", "", "Target UC Table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
target_table_location = dbutils.widgets.get("target_table_location")
# Variables mustn't be changed
source_catalog = "hive_metastore"
table_type = "table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.utils import get_table_description, clone_hms_table_to_uc

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
# MAGIC ## Migrating hive_metastore Tables to UC Managed or External Tables with data movement using the DEEP CLONE command
# MAGIC
# MAGIC Available options:
# MAGIC - Migrate all managed or external tables from the given `Source HMS Schema` to the given `Target UC Catalog` and `Target UC Schema`. 
# MAGIC   - Applicable if the `Source HMS Table(s)` is empty.
# MAGIC - Migrate managed or external table(s) from the given Hive Metastore `Source Schema` and `Source Table(s)` to the given `Target Catalog` and `Target Schema`.
# MAGIC   - Applicable if the `Source HMS Table(s)` is filled.
# MAGIC   - If `Target Table` is empty, the `Source HMS Table(s)`'s name is given to the Unity Catalog table.
# MAGIC - **Note**
# MAGIC   - To migrate as External Table, you need to fill out the `Target UC Table Location`

# COMMAND ----------

# Set empty sync status list
sync_status_list = []
# Iterate through table descriptions
for table_details in tables_descriptions:
    # Clone
    sync_status = clone_hms_table_to_uc(spark, dbutils, table_details, target_catalog, target_schema, target_table, target_table_location)
    # Append sync status list
    sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
    # If status code FAILED, exit notebook
    if sync_status.sync_status_code == "FAILED":
      dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)