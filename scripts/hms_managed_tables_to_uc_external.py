# Databricks notebook source
# MAGIC %md
# MAGIC # Hive Metastore Tables outside of DBFS to UC External Tables
# MAGIC
# MAGIC This notebook will migrate managed table(s) outside of DBFS in a given schema from the Hive Metastore to a UC catalog.
# MAGIC
# MAGIC **Functionality**:
# MAGIC - The source table(s) will be updated to an external table(s) without any data movement.
# MAGIC - The source table will be migrated with the SYNC command
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have external location(s) inplace for the table(s)' mounted file path(s) that you want to migrate.
# MAGIC - You have `CREATE EXTERNAL TABLE` privileges on the external location(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema.
# MAGIC * **`Source Table(s)`** (mandatory): 
# MAGIC   - The name of the source HMS table. Multiple tables should be given as follows "table_1, table_2".
# MAGIC * **`Target UC Catalog`** (mandatory):
# MAGIC   - The name of the target catalog.
# MAGIC * **`Target UC Schema`** (mandatory):
# MAGIC   - The name of the target schema.

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
# Variables mustn't be changed
source_catalog = "hive_metastore"
table_type = "table"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.utils import get_table_description, sync_table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert managed tables to external
# MAGIC
# MAGIC Type(s) of the source table(s) will be updated to EXTERNAL

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val tableNames = dbutils.widgets.get("source_table").split(", ")
# MAGIC val dbName = dbutils.widgets.get("source_schema")
# MAGIC
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC
# MAGIC for (tableName <- tableNames){
# MAGIC   println(s"Change $dbName.$tableName to external")
# MAGIC   val oldTable: CatalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName, Some(dbName)))
# MAGIC   val alteredTable: CatalogTable = oldTable.copy(tableType = CatalogTableType.EXTERNAL)
# MAGIC   spark.sessionState.catalog.alterTable(alteredTable)
# MAGIC }

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
  sync_status = sync_table(spark, table_details, target_catalog, target_schema, dry_run=False)
  # Append sync status list
  sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)