# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Hive Metastore user defined function(s) to UC function(s)
# MAGIC
# MAGIC This notebook will migrate used defined functions(s) from the Hive Metastore to a UC catalog.
# MAGIC
# MAGIC **Custom sync function**
# MAGIC
# MAGIC Syncing user defined functions between two catalogs.
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target function and recreating it
# MAGIC
# MAGIC **Note**: Before you start the migration, please double-check the followings:
# MAGIC - Check out the notebook logic
# MAGIC - You have migrated all the required schema(s) with the same name(s)
# MAGIC - You have migrated all the required table(s) with the same name(s) for the function(s)
# MAGIC - You have the right privileges on the UC catalog and schema securable objects
# MAGIC   - `CREATE TABLE`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget parameters
# MAGIC
# MAGIC * **`Source HMS Schema`** (mandatory): 
# MAGIC   - The name of the source HMS schema(s).
# MAGIC * **`Source HMS Function(s)`** (optional): 
# MAGIC   - The name of the source HMS function. Multiple functions should be given as follows "function_1, function_2". If filled only the given function(s) will be pulled otherwise all the functions.
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
dbutils.widgets.text("source_function", "", "Source HMS Function")
dbutils.widgets.text("target_catalog", "", "Target UC Catalog")
dbutils.widgets.text("target_schema", "", "Target UC Schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract widgets values

# COMMAND ----------

source_schema = dbutils.widgets.get("source_schema")
source_function = dbutils.widgets.get("source_function")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema =  dbutils.widgets.get("target_schema")
# Variables mustn't be changed
source_catalog = "hive_metastore"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import modules

# COMMAND ----------

from utils.utils import get_user_defined_function_description, sync_function

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the hive metastore function(s)' descriptions
# MAGIC
# MAGIC Available options:
# MAGIC - Get all functions descriptions if the `Source HMS Function(s)` parameter is empty
# MAGIC - Get the given function(s) description if the `Source HMS Function(s)` is filled

# COMMAND ----------

function_descriptions = get_user_defined_function_description(spark, source_catalog, source_schema, source_function)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync the Hive Metastore function(s) to Unity Catalog
# MAGIC
# MAGIC **Functionality**:
# MAGIC - Replacing the target view and recreating it

# COMMAND ----------

# Create empty sync status list
sync_status_list = []
# Iterate through the function descriptions
for function_details in function_descriptions:
  # Sync function
  sync_status = sync_function(spark, function_details,target_catalog, target_schema)
  # Append sync status list
  sync_status_list.append([sync_status.source_object_type, sync_status.source_object_full_name, sync_status.target_object_full_name, sync_status.sync_status_code, sync_status.sync_status_description])
  # If sync status code FAILED, exit notebook
  if sync_status.sync_status_code == "FAILED":
    dbutils.notebook.exit(sync_status_list)

# Exit notebook
dbutils.notebook.exit(sync_status_list)