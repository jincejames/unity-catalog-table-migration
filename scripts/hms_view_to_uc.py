# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Hive Metastore View(s) to UC view(s)
# MAGIC
# MAGIC This notebook will migrate view(s) from the Hive Metastore to a UC catalog.
# MAGIC
# MAGIC **Custom sync function**
# MAGIC
# MAGIC Syncing views between two catalogs.
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it if:
# MAGIC   - The source and target view definitions are different
# MAGIC - Create the view if it doesn't exist
# MAGIC
# MAGIC **IMPORTANT**:
# MAGIC - It is **only migrating views to a single target catalog**
# MAGIC - The **Schema(s) has to exist with the same name in Unity Catalog**
# MAGIC - The **tables with the same name have to exist in Unity Catalog within their same schema** as in the Hive Metastore
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have migrated all the required schema(s) with the same name(s)
# MAGIC - You have migrated all the required table(s) with the same name(s) for the view(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `USE CATALOG`
# MAGIC   - `USE SCHEMA`
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source HMS Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s).
# MAGIC * **`Source HMS View(s)`** (optional): 
# MAGIC   - The name of the source HMS view. Multiple views should be given as follows "view_1, view_2". If filled only the given view(s) will be pulled otherwise all the views.
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
dbutils.widgets.text("source_schema", "", "Source HMS Schema")
dbutils.widgets.text("source_view", "", "Source HMS View")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_view = dbutils.widgets.get("source_view")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema =  dbutils.widgets.get("target_schema")
# Variables mustn't be changed
source_catalog = "hive_metastore"
table_type = "view"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.utils import get_table_description, sync_view

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore vies(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all views descriptions if the `Source HMS View(s)` parameter is empty
# MAGIC - Get the given view(s) description if the `Source HMS View(s)` is filled

# COMMAND ----------

view_descriptions = get_table_description(spark, source_catalog, source_schema, source_view, table_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the Hive Metastore view(s) to Unity Catalog
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it if:
# MAGIC   - The source and target view definitions are different
# MAGIC - Create the view if it doesn't exist
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - The **Schema(s) has to exist with the same name in Unity Catalog**
# MAGIC - The **tables with the same name have to exist in Unity Catalog within their same schema** as in the Hive Metastore

# COMMAND ----------

# Create empty sync status list
sync_status_list = []
# Iterate through the view descriptions
for view_details in view_descriptions:
  # Sync view
  sync_status = sync_view(spark, view_details, target_catalog)
  # Append sync status list
  sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)

# COMMAND ----------

