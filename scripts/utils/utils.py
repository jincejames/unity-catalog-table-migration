from dataclasses import dataclass
import re
import traceback

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

import pyspark.sql.functions as F
import pyspark.sql.utils as U


@dataclass
class SyncStatus:
    requested_object_name: str = None
    source_catalog_name: str = None
    target_catalog_name: str = None
    source_schema_name: str = None
    target_schema_name: str = None
    table_location: str = None
    source_object_full_name: str = None
    target_object_full_name: str = None
    source_object_type: str = None
    source_table_format: str = None
    source_table_schema: str = None
    source_view_text: str = None
    sync_status_code: str = None
    sync_command_status_code = None
    sync_status_description: str = None


@dataclass
class FunctionDLL:
  function_class: str = None
  name: str = None
  body: str = None
  returns: list[str] = None
  input_parameters: list[str] = None
  language: str = None
  deterministic: str = None
  function_type: str = None
  comment: str = None


@dataclass
class TableDDL:
  requested_object_name: str = None
  catalog_name: str = None
  schema_name: str = None
  original_ddl: str = None
  full_table_name: str = None
  table_schema: str = None
  using: str = None
  partition_by: str = None
  cluster_by: str = None
  table_properties: str = None
  comment: str = None
  options: str = None
  location: str = None
  dynamic_ddl: str = None


def get_table_description(spark: SparkSession, source_catalog: str, source_schema: str, source_table: str, table_type: str) -> list:
  """
  Get the Descriptions of HMS tables/views to a list
  
  Parameters:
    spark: Active SparkSession
    source_catalog: The name of the source catalog.
    source_schema: The name of the source schema
    source_table: The name(s) of the source table(s) in the given schema. Should be given as a string like "table_1, table_2". All the tables in the given source schema will be used if not given.
    table_type: The type of the source table (e.g. EXTERNAL, VIEW)
  
  Returns:
    The list of dictionaries of the tables' descriptions.
  """

  try:

    if not source_schema:
      raise ValueError("Source Schema is empty")

    # List variable for table descriptions
    table_descriptions = []

    if not source_table:
      # Read all tables in the given schema to DataFrame
      source_tables = (spark
                    .sql(f"show tables in {source_catalog}.{source_schema}")
                    .select("tableName")
                    )
      # Get all tables from Hive Metastore for the given hive_metastore schema
      tables = list(map(lambda r: r.tableName, source_tables.collect()))

    elif source_table.find(","):
      # The input is a list of tables in a string
      tables = source_table.split(", ")
    else:
      # The input is a single table
      tables = [f"{source_table}"]

    # Loop through each table and run the describe command
    for table in tables:
        table_name = table
        full_table_name = f"{source_catalog}.{source_schema}.{table_name}"
        print(f"Getting {full_table_name} descriptions using DESCRIBE FORMATTED command")
        # Read the Table description into DataFrame
        desc_df = (spark
                  .sql(f"DESCRIBE FORMATTED {full_table_name}")
                  )
        
        # Create description dict
        desc_dict = {row['col_name']:row['data_type'] for row in desc_df.collect()}
        
        # Filter table type
        # If table type view, filter for view type
        if table_type == "view":
          
          if desc_dict["Type"].lower() == "view":
            print(f"{desc_dict['Table']} is a view, appending...")
            # Append table_descriptions
            table_descriptions.append(desc_dict)
        
        # Filter table type to non-view  
        elif desc_dict['Type'].lower() != "view": 
            print(f"{desc_dict['Table']} is a table, appending...")
            # Append table_descriptions
            table_descriptions.append(desc_dict)
      
  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise ValueError(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      print(f"ValueError occurred: {e}")
      raise U.AnalysisException(f"AnalysisException occurred: {e}")
  return table_descriptions


def clone_hms_table_to_uc(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, target_table: str = "", target_location: str = "") -> SyncStatus:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the DEEP CLONE command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table:(optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.
  Returns:
    A SyncStatus object that contains the sync status of the table
  
  """
  try:

    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
        raise ValueError("table_details input list is empty")
    elif not target_catalog:
        raise ValueError("target_catalog input string is empty")

    # Set hive_metastore variables
    hms_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = hms_catalog
    hms_schema = table_details["Database"]
    sync_status.source_schema_name = hms_schema
    hms_table = table_details["Table"]
    sync_status.requested_object_name = hms_table
    hms_table_provider = table_details["Provider"]
    sync_status.source_table_format = hms_table_provider
    hms_table_type = table_details["Type"]
    sync_status.source_object_type = hms_table_type
    
    # Set UC variables
    uc_catalog = target_catalog
    sync_status.target_catalog_name = uc_catalog
    uc_schema = target_schema if target_schema else hms_schema
    sync_status.target_schema_name = uc_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table

    # Set HMS table full name
    hms_full_name = f"{hms_catalog}.{hms_schema}.{hms_table}"
    sync_status.source_object_full_name = hms_full_name
    # Set UC table full name
    uc_full_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    sync_status.target_object_full_name = uc_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {hms_full_name}")

    # Set DEEP CLONE COMMAND
    deep_clone_statement = f"""
                        CREATE OR REPLACE TABLE {uc_full_name}
                        DEEP CLONE {hms_full_name}
                        """

    print("Migrating with CLONE ...")
    
    # If target location is given, add LOCATION to the deep clone statement and create external table
    if target_location:
        print(f"Migrating {hms_table_type} {hms_full_name} table to External {uc_full_name} in location {target_location} using DEEP CLONE")
        deep_clone_statement += f" LOCATION '{target_location}'"
        
        sync_status.sync_status_description = f"External table has been created using DEEP CLONE"

        print(f"Executing '{deep_clone_statement}'")
        spark.sql(deep_clone_statement)

    else:
        print(f"Migrating {hms_table_type} {hms_full_name} table to Managed {uc_full_name} in location {target_location} using DEEP CLONE")
          
        sync_status.sync_status_description = f"Managed table has been created using DEEP CLONE"

        print(f"Executing '{deep_clone_statement}'")
        spark.sql(deep_clone_statement)

    # Set current user and current timestamp
    current_u = spark.sql("SELECT current_user()").collect()[0][0]
    current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
    # Extract the notebook name
    notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]


    # Set table properties if it's delta
    if hms_table_provider.lower() == "delta":
        print("Setting TBLPROPERTIES ... ")
        set_table_properties_statement = f"""
                                        ALTER TABLE {hms_full_name} 
                                        SET TBLPROPERTIES ('cloned_to' = '{uc_full_name}',
                                                        'cloned_by' = '{current_u}',
                                                        'cloned_at' = '{current_t}',
                                                        'cloned_type' = '{sync_status.sync_status_description} via {notebook_name}')
                                        """
        print(f"Executing '{set_table_properties_statement}' ...")
        # Execute set table properties
        spark.sql(set_table_properties_statement)

    # Check the match of hive_metastore and UC tables
    check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)

    sync_status.sync_status_code = "SUCCESS"

    print(sync_status.sync_status_description)
  
  except Exception:
      sync_status.sync_status_code = "FAILED"
      sync_status.sync_status_description = str(traceback.format_exc())
      print(str(traceback.format_exc()))
  return sync_status

def check_equality_of_hms_uc_table(spark: SparkSession, hms_schema: str, hms_table: str, uc_catalog: str, uc_schema: str, uc_table: str) -> None:
  """
  Checking the migrated UC Table equality to its source HMS table by the volume and the schema of the tables
  
  Parameters:
    spark: Active SparkSession
    hms_schema: The name of the schema in the hive_metastore
    hms_table: The name of the table in the given hive_metastore schema
    uc_catalog: The name of the target Unity Catalog catalog
    uc_schema: The name of the schema Unity Catalog schema
    uc_table: The name of the target Unity Catalog table

  """
  try:
    if not hms_schema:
      raise ValueError(f"Source hive_metastore Schema is not given")
    elif not hms_table:
      raise ValueError(f"Source hive_metastore Table is not given")
    elif not uc_catalog:
      raise ValueError(f"Target UC Catalog is not given")
    elif not uc_schema:
      raise ValueError(f"Target UC Schema is not given")
    elif not uc_table:
      raise ValueError(f"Target UC Table is not given")
    
    # Set hms_catalog variable
    hms_catalog = "hive_metastore"

    # Read hive_metastore and UC table as DataFrame
    hms = spark.read.table(f"{hms_catalog}.{hms_schema}.{hms_table}")
    uc = spark.read.table(f"{uc_catalog}.{uc_schema}.{uc_table}")

    # Extract HMS and UC schemas and volumes
    hms_schema_struct = hms.schema
    uc_schema_struct = uc.schema
    hms_count = hms.count()
    uc_count = uc.count()

    print(f"Check source and target tables equality...")

    # Check schema match
    if hms_schema_struct == uc_schema_struct:
      # Check volume match
      if hms_count == uc_count:
        print(f"{hms_catalog}.{hms_schema}.{hms_table} has been created to {uc_catalog}.{uc_schema}.{uc_table} correctly")
      else:
        print(f"{hms_catalog}.{hms_schema}.{hms_table} count {hms_count} not equals with {uc_catalog}.{uc_schema}.{uc_table} count {uc_count}. Diff: {hms_count-uc_count}")
        raise AssertionError("Data volumes are not matching")
    else:
      print(f"{hms_catalog}.{hms_schema}.{hms_table} schema not equals with {uc_catalog}.{uc_schema}.{uc_table} schema. Diff: {list(set(hms_schema_struct)-set(uc_schema_struct))}")
      raise AssertionError("Schemas are not matching")

  except (ValueError, AssertionError) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, AssertionError):
      print(f"AssertionError occurred: {e}")
      raise e


def get_create_table_stmt(spark: SparkSession, catalog: str, schema: str, table: str) -> TableDDL:
  """
  Getting the create table statement
  
  Parameters:
    spark: Active SparkSession
    catalog: The name of the catalog
    schema: The name of the schema
    table: The name of the table

  Returns:
    The TableDDL object
  """
  try:

    if not catalog:
      raise ValueError("catalog input string is empty")
    elif not schema:
      raise ValueError("schema input string is empty")
    elif not table:
      raise ValueError("table input string is empty")

    table_ddl = TableDDL()

    # Set full table name
    full_table_name = f"{catalog}.{schema}.{table}"

    # Create table definition DataFrame
    table_definition_df = (spark.sql(f"SHOW CREATE TABLE {full_table_name}")
                            .withColumn("createtab_stmt", F.regexp_replace(F.col("createtab_stmt"), "\n", " "))
                            .withColumn("full_table_name", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) \(", 2))
                            .withColumn("location", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"LOCATION '([^']+)'", 1))
                            .withColumn("schema", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) +\((.+?)\)", 3))
                            .withColumn("using",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"(?<=USING\s)(\w+)(?=\s)", 0))
                            .withColumn("partition_by",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"PARTITIONED BY \((.*?)\)", 1))
                            .withColumn("cluster_by",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"CLUSTER BY \((.*?)\)", 1))
                            .withColumn("table_properties",
                                        F.regexp_extract(F.col("createtab_stmt"),
                                                        r"TBLPROPERTIES \(\s*(.*?)\s*\)", 1))
                            .withColumn("comment", 
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"COMMENT '([^']+)'", 1))
                            .withColumn("options",
                                        F.regexp_extract(F.col("createtab_stmt"), 
                                                        r"OPTIONS \((.*?)\)", 1))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("createtab_stmt"), 
                                                        r"^CREATE (TABLE|VIEW) ([A-Za-z0-9._]+) \(", "CREATE $1 <<full_table_name>> ("))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("dynamic_ddl"), 
                                                        r"LOCATION '([^']+)'", "LOCATION '<<location>>'"))
                            .withColumn("dynamic_ddl", 
                                        F.regexp_replace(F.col("dynamic_ddl"), 
                                                        r"^CREATE (TABLE|VIEW) ([^(]+)\((.+?)\)", "CREATE $1 $2 (<<schema>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"(?<=USING\s)(\w+)(?=\s)", " <<using>>"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"PARTITIONED BY \((.*?)\)", "PARTITIONED BY (<<partition_by>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"COMMENT '([^']+)'", "COMMENT '<<comment>>'"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"CLUSTER BY \((.*?)\)", "CLUSTER BY (<<cluster_by>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"TBLPROPERTIES \(\s*(.*?)\s*\)", "TBLPROPERTIES(<<table_properties>>)"))
                            .withColumn("dynamic_ddl",
                                        F.regexp_replace(F.col("dynamic_ddl"),
                                                        r"OPTIONS \((.*?)\)", "OPTIONS (<<options>>)"))
                            )
    
    # Set table_ddl attributes

    table_ddl.requested_object_name = table
    table_ddl.catalog_name = catalog
    table_ddl.schema_name = schema
    table_ddl.original_ddl = table_definition_df.select("createtab_stmt").collect()[0][0]
    table_ddl.full_table_name = table_definition_df.select("full_table_name").collect()[0][0]
    table_ddl.table_schema = table_definition_df.select("schema").collect()[0][0]
    table_ddl.using = table_definition_df.select("using").collect()[0][0]
    table_ddl.partition_by = table_definition_df.select("partition_by").collect()[0][0]
    table_ddl.cluster_by = table_definition_df.select("cluster_by").collect()[0][0]
    table_ddl.table_properties = table_definition_df.select("table_properties").collect()[0][0]
    table_ddl.comment = table_definition_df.select("comment").collect()[0][0]
    table_ddl.options = table_definition_df.select("options").collect()[0][0]
    table_ddl.location = table_definition_df.select("location").collect()[0][0]
    table_ddl.dynamic_ddl = table_definition_df.select("dynamic_ddl").collect()[0][0]

    return table_ddl

  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise e
    elif isinstance(e, U.AnalysisException):
      print(f"AnalysisException occurred: {e}")
      raise e


def ctas_hms_table_to_uc(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, target_table: str = "", select_statement: str = "", partition_clause: str = "", options_clause: str = "", target_location: str = "", file_format: str = "") -> SyncStatus:
  """
  Migrating hive_metastore Table(s) to UC Table(s) either EXTERNAL OR MANAGED with data movement using the CREATE OR REPLACE TABLE AS SELECT (CTAS) command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    table_details: The dictionary of the table descriptions.
    target_catalog: The name of the target Unity Catalog catalog
    target_schema: The name of the target Unity Catalog schema
    target_table: (optional) The name of the target Unity Catalog table. If not given UC table gets the name of the original HMS table.
    select_statement: (optional) User-defined select statement. SELECT and FROM don't need to define.
    partition_clause: (optional) An optional clause to partition the table by a subset of columns.
    options_cluse: (optional) User-defined options including comment and tblproperties.
    target_location (optional) The target location path
    file_format: (optional) The file format of the target table

  Returns:
    A SyncStatus object that contains the sync status of the table

  """
  try:

    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
      raise ValueError("table_details input dict is empty")
    elif not target_catalog:
      raise ValueError(f"target_catalogis not given")

    # Set hive_metastore variables
    hms_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = hms_catalog
    hms_schema = table_details["Database"]
    sync_status.source_schema_name = hms_schema
    hms_table = table_details["Table"]
    sync_status.requested_object_name = hms_table
    hms_table_provider = table_details["Provider"]
    sync_status.source_table_format = hms_table_provider
    hms_table_type = table_details["Type"]
    sync_status.source_object_type = hms_table_type

    # Get HMS table DDL
    hms_table_ddl = get_create_table_stmt(spark, hms_catalog, hms_schema, hms_table)

    # Extract hms table ddl clauses
    using = hms_table_ddl.using
    options = hms_table_ddl.options
    partition_by = hms_table_ddl.partition_by
    cluster_by = hms_table_ddl.cluster_by
    location = hms_table_ddl.location
    comment = hms_table_ddl.comment
    table_properties = hms_table_ddl.table_properties

    # Set UC variables
    uc_catalog = target_catalog
    uc_schema = target_schema if target_schema else hms_schema
    # I target UC table is not given, use the hive_metastore table
    uc_table = target_table if target_table else hms_table

    # Set HMS and UC full table name
    hms_full_name = f"{hms_catalog}.{hms_schema}.{hms_table}"
    sync_status.source_object_full_name = hms_full_name
    uc_full_name = f"{uc_catalog}.{uc_schema}.{uc_table}"
    sync_status.target_object_full_name = uc_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {hms_full_name}")

    print(f"Migrating with CTAS ...")

    # Set the base CTAS statement
    create_table_as_statement = f"CREATE OR REPLACE TABLE {uc_full_name}"

    # using
    if file_format:
        create_table_as_statement += f" USING {file_format}"
    elif using:
        create_table_as_statement += f" USING {using}"
    elif hms_table_provider:
        create_table_as_statement += f" USING {hms_table_provider}"

    # table_clauses
    
    # OPTIONS
    if options_clause:
      create_table_as_statement += f" OPTIONS ({options_clause})"
    elif options:
        create_table_as_statement += f" OPTIONS ({options})"

    # PARTITION BY or CLUSTER BY
    if partition_clause:
      create_table_as_statement += f" PARTITIONED BY ({partition_clause})"
    elif partition_by:
        create_table_as_statement += f" PARTITIONED BY ({partition_by})"
    elif cluster_by:
        create_table_as_statement += f" CLUSTER BY ({cluster_by})"

    # LOCATION
    if target_location:
      create_table_as_statement += f" LOCATION '{target_location}'"

    # COMMENT
    if comment:
        create_table_as_statement += f" COMMENT '{comment}'"

    # TBLPROPERTIES
    if table_properties:
        create_table_as_statement += f" TBLPROPERTIES({table_properties})"

    # SELECT STATEMENT
    if not select_statement:
      select_statement = f" AS SELECT * FROM {hms_full_name};"

    # Final CTAS
    create_table_as_statement += select_statement

    print(f"Executing '{create_table_as_statement}' ...")

    spark.sql(create_table_as_statement)
    print(f"Table {hms_full_name} migrated to {uc_full_name} with CTAS")

    # Set table properties if it's delta
    if hms_table_provider.lower() == "delta":
      print("Setting TBLPROPERTIES ...")
      # Set current user and current timestamp
      current_u = spark.sql("SELECT current_user()").collect()[0][0]
      current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
      # Extract the notebook name
      notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]
      
      # Set table properties statement
      tblproperties_stmt = f"""
                          ALTER TABLE {hms_full_name} 
                          SET TBLPROPERTIES ('upgraded_to' = '{uc_catalog}.{uc_schema}.{uc_table}',
                                            'upgraded_by' = '{current_u}',
                                            'upgraded_at' = '{current_t}',
                                            'upgraded_type' = '{notebook_name} using CTAS'
                                            )
                          """
      # Execute set table properties
      print(f"Executing 'tblproperties_stmt' ...")
      spark.sql(tblproperties_stmt)
    else:
      print(f"Table provider is {hms_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
      comment_stmt = f"COMMENT ON TABLE {hms_full_name} IS 'Upgraded to {uc_full_name} by {current_u} at {current_t} via {notebook_name} using CTAS.'"
      print(f"Executing '' ...")
      spark.sql(comment_stmt)

    # Check the match of hive_metastore and UC tables
    check_equality_of_hms_uc_table(spark, hms_schema, hms_table, uc_catalog, uc_schema, uc_table)

    # Set sync status
    sync_status.sync_status_description = f"Table has been created using CTAS statement {create_table_as_statement}"
    sync_status.sync_status_code = "SUCCESS"

    print(f"Migrating table via CTAS has successfully finished")
      
  except Exception:
    sync_status.sync_status_code = "FAILED"
    sync_status.sync_status_description = str(traceback.format_exc())
    print(str(traceback.format_exc()))
  
  return sync_status


def sync_table(spark: SparkSession, table_details: dict, uc_catalog: str, uc_schema: str, dry_run: bool=True) -> SyncStatus:
  """
  Migrating hive_metastore external table to UC Table with SYNC command.
  
  Parameters:
      spark: The active SparkSession
      table_details: Table description
      uc_catalog: The name of the target Unity Catalog catalog
      uc_schema: The name of the target Unity Catalog schema
  
  Returns:
      SyncStatus object that contains the sync status of the table

  """
  try:
    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
      raise ValueError("view_details input dict is empty")
    if not uc_catalog:
      raise ValueError("target catalog input string is empty")
    if not uc_schema:
      raise ValueError("target schema input string is empty")

    #Extract table variables and set sync status
    source_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = source_catalog
    source_schema = table_details["Database"]
    sync_status.source_schema_name = source_catalog
    source_table = table_details["Table"]
    sync_status.requested_object_name = source_table

    # Set HMS table full name
    hms_full_name = f"{source_catalog}.{source_schema}.{source_table}"
    sync_status.source_object_full_name = hms_full_name
    # Set UC table full name
    uc_full_name = f"{uc_catalog}.{uc_schema}.{source_table}"
    sync_status.target_object_full_name = uc_full_name

    sync_statement = f"SYNC TABLE {uc_full_name} FROM {hms_full_name}"

    # If dry run is True append it
    if dry_run:
      print(f"Migrating {hms_full_name} table to External {uc_full_name} using DRY RUN")
      sync_statement += f" DRY RUN"
    else:
      print(f"Migrating {hms_full_name} table to External {uc_full_name}")

    print(f"Executing '{sync_statement}'")
    sync_result = spark.sql(sync_statement).collect()[0].asDict()

    sync_status.requested_object_name = sync_result["target_name"]
    sync_status.sync_command_status_code = sync_result["status_code"]
    sync_status.sync_status_description = sync_result["description"]

    if sync_result["status_code"] in ["DRY_RUN_SUCCESS", "SUCCESS"]:
      sync_status.sync_status_code = "SUCCESS"
    else:
      sync_status.sync_status_code = "FAILED"

  except Exception:
    sync_status.sync_status_code = "FAILED"
    sync_status.sync_status_description = str(traceback.format_exc())
    print(str(traceback.format_exc()))
    return sync_status
  
  return sync_status


def get_the_object_external_location(dbutils: DBUtils, object_type: str, location: str) -> str:
  """
  Get the cloud storage path of the object location as an external location.

  Parameters:
    dbutils: Databricks Utilities
    object_type: Object type name, either 'table', 'schema', or 'catalog'.
    location: The location of the object

  Returns
    The cloud storage path as external location of the object location
  """

  try:
    if not object_type:
      raise ValueError("object_type input string is empty")
    elif not location:
      raise ValueError("location input string is empty")

    # Extract the mounts
    mounts = dbutils.fs.mounts()

    # Iterate through the mounts and return the external location
    for mount in mounts:
      if location.startswith(f"dbfs:{mount.mountPoint}/"):
        # Extract the mount point location
        external_location = location.replace(f"dbfs:{mount.mountPoint}/", mount.source).replace(f"%20", " ")
        break

      elif location[0:6] == "dbfs:/":
        
        if object_type.lower() == "table":
          # Pass, let gets the default location
          pass
        else:
          # Set database/schema/catalog location to None if it is on DBFS
          external_location = None
      
      else:
    
        # Set the external location to the current database/schema location
        external_location = location
      
    # Set external location to location if the location does not exist as mount point
    if object_type == "table":
      # Set external location if it is not DBFS
      external_location = external_location if external_location else location
      if external_location[0:6] == "dbfs:/":
        # Raise error if location is DBFS
        raise ValueError(f"The location {location} is DBFS. It needs to be migrated away with data movement")
    else:
      # Set location that calculated in the loop
      external_location

    return external_location

  except ValueError as e:
    print(f"ValueError occurred: {e}")
    raise e


def sync_table_as_external(spark: SparkSession, dbutils: DBUtils,table_details: dict, target_catalog: str, target_schema: str, target_table: str = None) -> SyncStatus:
  """
  Syncs table(s) as External Table(s) between two catalogs.

  Parameters:
    spark: Active SparkSession
    table_details: The details of the table
    target_catalog: The name of the target catalog
    target_schema: The name of the target schema
    target_table: (optional) The name of the target table. If not given, the table gets the name of the source table. Only applicable if there is a single source table given.
  
  Returns:
    SyncStatus object that contains the synced tables

  """
  try:

    # Set sync status variable from the SyncStatus object
    sync_status = SyncStatus()

    if not table_details:
      raise ValueError("table_details input dict is empty")
    elif not target_catalog:
      raise ValueError("target_catalog input list is empty")

    # Set source variables
    source_catalog = table_details["Catalog"]
    source_schema = table_details["Database"]
    source_table = table_details["Table"]
    source_table_type = table_details["Type"]
    source_table_provider = table_details["Provider"]
    source_table_location = table_details["Location"]
    source_table_partitions = table_details["# Partition Information"] if "# Partition Information" in table_details else ""

    # Set default sync status attributes
    sync_status.requested_object_name=source_table,
    sync_status.source_catalog_name=source_catalog,
    sync_status.target_catalog_name=target_catalog,
    sync_status.source_schema_name=source_schema,
    sync_status.source_object_type=source_table_type,
    sync_status.source_table_format=source_table_provider

    # Set target variables
    target_schema = target_schema if target_schema else source_schema
    sync_status.target_schema_name = target_schema
    target_table = target_table if target_table else source_table
    target_table_location = get_the_object_external_location(dbutils, "table", source_table_location)
    sync_status.table_location = target_table_location

    # Set source and target tables full name
    source_full_name = f"{source_catalog}.{source_schema}.{source_table}"
    sync_status.source_object_full_name = source_full_name
    target_full_name = f"{target_catalog}.{target_schema}.{target_table}"
    sync_status.target_object_full_name = target_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {source_full_name}")

    print(f"Syncing table...")

    # Check if target table exists
    target_table_exists = True if [t for t in spark.sql(f"SHOW TABLES in {target_catalog}.{target_schema}").filter((F.col("isTemporary") == 'false') & (F.col("tableName") == f'{target_table}')).collect()] else False

    if target_table_exists:

      print("Target table exists. Checking for table changes in schema or format...")

      # Clear all caches first
      spark.sql(f"REFRESH TABLE {source_full_name}")

      # Set drop target table variable to False
      drop_target_table = False

      # Extract the Target table provider and type
      target_table_description_df = (spark.sql(f"DESCRIBE TABLE EXTENDED {target_full_name}"))
      target_table_provider = (target_table_description_df
                                .filter("col_name = 'Provider'")
                                .select("data_type")
                                .collect()[0]["data_type"]
                                ).lower()
      if not target_table_provider:
        raise ValueError(f"The table {target_full_name} format could not find using DESCRIBE TABLE EXTENDED")

      target_table_type = (target_table_description_df
                                .filter("col_name = 'Type'")
                                .select("data_type")
                                .collect()[0]["data_type"]
                                ).lower()
      if not target_table_type:
        raise ValueError(f"The table {target_full_name} type could not find using DESCRIBE TABLE EXTENDED")
      
      # If formats are different, mark the target table for drop
      if source_table_provider != target_table_provider:
        print(
                  f"Target table '{target_full_name}' format '{target_table_provider}' does not match with source table '{source_full_name}' format '{source_table_provider}', marking '{target_full_name}' for drop"
              )
        sync_status.sync_status_description = f"DROP and CREATE table due to differences in formats (target is '{target_table_provider}', source is '{source_table_provider}')"
        drop_target_table = True

      # If formats are same, continue to check for schema differences
      if not drop_target_table:

        # Get the Source and Target tables' schemas
        source_table_schema = spark.sql(f"SELECT * FROM {source_full_name} LIMIT 0").schema
        sync_status.source_table_schema = source_table_schema.json()
        target_table_schema = spark.sql(f"SELECT * FROM {target_full_name} LIMIT 0").schema
        
        # If schemas are different, mark the target table for drop
        if source_table_schema != target_table_schema:
            print(
                f"Target table '{target_full_name}' schema does not match with source table '{source_full_name}', marking '{target_full_name}' for drop"
            )

            sync_status.sync_status_description = f"DROP and CREATE TABLE due to differences in schemas (target is '{target_table_schema.json()}', source is '{source_table_schema.json()}')"

            drop_target_table = True

      # Drop the target table if format or schema are different and its an external table
      if drop_target_table:
        if target_table_type == "external":
          print(f"Dropping {target_full_name} table...")
          spark.sql(f"DROP TABLE {target_full_name}")
        
        else:
          raise ValueError(f"{target_full_name} is not an external table.")          

    # Create external table if target table doesn't exist or recently dropped
    if not target_table_exists or drop_target_table:
      
      if not target_table_exists:
        print(f"{target_full_name} doesn't exist. Creating...")
        sync_status.sync_status_description = ("Target table does not exist, CREATE a new table")
      elif drop_target_table:
        print(f"{target_full_name} exists but not matches to the {source_full_name}. Syncing...")
        sync_status.sync_status_description = ("Target table does not match to the source table, Sync target table")

      # Set current user and current timestamp
      current_u = spark.sql("SELECT current_user()").collect()[0][0]
      current_t = spark.sql("SELECT current_timestamp()").collect()[0][0]
      # Extract the notebook name
      notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[-1]
      
      # Create Delta Table
      if source_table_provider.lower() == "delta":
        
        print("Creating Delta Table")

        # Set create external delta table statement
        create_delta_table_statement = f"CREATE TABLE IF NOT EXISTS {target_full_name} USING {source_table_provider} LOCATION '{target_table_location}'"

        print(f"Executing '{create_delta_table_statement}' ...")
        spark.sql(create_delta_table_statement)
        
        # Execute set table properties
        print("Setting TBLPROPERTIES...") 
        set_delta_table_properties_statement = (f"""
                                                ALTER TABLE {source_full_name} 
                                                SET TBLPROPERTIES ('synced_to' = '{target_full_name}',
                                                                  'synced_by' = '{current_u}',
                                                                  'synced_at' = '{current_t}',
                                                                  'synced_type' = '{notebook_name} using custom sync_table function')
                                                """
                                                )
        print(f"Executing '{set_delta_table_properties_statement}' ...")
        spark.sql(set_delta_table_properties_statement)

        sync_status.sync_status_description = ("DELTA table has successfully created")

      else:
        # Create Non-Delta Table

        print(f"Creating {source_table_provider} Table")

        # Extract the source table schema as DDL
        source_table_schema_ddl = (
            spark.sql(f"SELECT * FROM {source_full_name} LIMIT 0")
            ._jdf.schema()
            .toDDL()
        )
        
        # Set create external non-delta table statement
        create_non_delta_table_statement = f"CREATE TABLE IF NOT EXISTS {target_full_name} ({source_table_schema_ddl}) USING {source_table_provider} LOCATION '{target_table_location}'"
        
        print(f"Executing '{create_non_delta_table_statement}' ...")
        spark.sql(create_non_delta_table_statement)
        
        print(f"Table provider is {source_table_provider}, hence TBLPROPERTIES cannot be set. Setting table comment instead.")
        
        # Set comment to created
        comment = f"Synced to {target_full_name} by {current_u} at {current_t} via {notebook_name} using custom sync_table function."
        comment_statement=(f"COMMENT ON TABLE {source_full_name} IS '{comment}'")
        print(f"Executing '{comment_statement}' ...")
        spark.sql(comment_statement)

        sync_status.sync_status_description = (f"{source_table_provider} table has successfully created")

      # Check the match of hive_metastore and UC tables
      check_equality_of_hms_uc_table(spark, target_schema, target_table, source_catalog, source_schema, source_table)

    else:
      print(f"No action required as tables '{target_full_name}' and '{source_full_name}' are already in sync")
      sync_status.sync_status_description = ("No action required as tables are already in sync")

    # Do a final metadata sync
    print(f"Doing a final MSCK REPAIR and/or REFRESH on '{target_full_name}'")
    if target_table_exists and target_catalog != "hive_metastore" and target_table_provider == "delta":
        msck_repair_sync_metadata_statement = f"MSCK REPAIR TABLE {target_full_name} SYNC METADATA"
        print(f"Executing '{msck_repair_sync_metadata_statement}'")
        spark.sql(msck_repair_sync_metadata_statement)
    else:
      if target_catalog == "hive_metastore" and source_table_partitions:
        msck_repair_statement = f"MSCK REPAIR TABLE {target_full_name}"
        print(f"Executing '{msck_repair_statement}'")
        spark.sql(msck_repair_statement)
      
    refresh_statement = f"REFRESH TABLE {target_full_name}"
    print(f"Executing '{refresh_statement}'")
    spark.sql(refresh_statement)

    sync_status.sync_status_code = "SUCCESS"
          
  except Exception:
    sync_status.sync_status_code = "FAILED"
    sync_status.sync_status_description = str(traceback.format_exc())
  return sync_status


def sync_view(spark: SparkSession, view_details: dict, target_catalog: str) -> SyncStatus:
  """
  Sync views between catalogs
  
  Parameters:
      spark: The active SparkSession
      view_details: View description
      target_catalog: The name of the target catalog.
  
  Returns:
      SyncStatus object that contains the sync status of the view

  """
  try:

      # Set sync status variable from the SyncStatus object
      sync_status = SyncStatus()

      if not view_details:
          raise ValueError("view_details input dict is empty")
      elif not target_catalog:
          raise ValueError("target catalog input string is empty")
          
      #Extract view variables and set sync status
      source_catalog = view_details["Catalog"]
      sync_status.source_catalog_name = source_catalog
      source_schema = view_details["Database"]
      sync_status.source_schema_name = source_catalog
      source_view = view_details["Table"]
      sync_status.requested_object_name = source_view
      source_view_definition = view_details["View Text"]
      sync_status.source_view_text = source_view_definition

      print(f"Syncing view...")
      # Currently, replacing hive_metastore with the target catalog
      print(f"Replacing {source_catalog} with {target_catalog}")
      source_view_definition = source_view_definition.replace(source_catalog, target_catalog)

      # Set target variables and sync status
      target_schema = source_schema
      sync_status.target_schema_name = target_schema
      target_view = source_view

      # Set full view name and sync status
      source_full_name = f"{source_catalog}.{source_schema}.{source_view}"
      sync_status.source_object_full_name = source_full_name
      target_full_name = f"{target_catalog}.{source_schema}.{target_view}"
      sync_status.target_object_full_name = target_full_name

      print(f"Syncing view '{source_full_name}' to '{target_full_name}'")

      # Clear all caches first
      spark.sql(f"REFRESH TABLE {source_full_name}")

      # Set catalog
      use_catalog_stmt = f"USE CATALOG {target_catalog}"
      print(f"Executing {use_catalog_stmt}...")
      spark.sql(use_catalog_stmt)

      # Check if target view exists
      target_view_exists = True if [t for t in spark.sql(f"SHOW VIEWS in {target_schema}").filter((F.col("isTemporary") == 'false') & (F.col("viewName") == f'{target_view}')).collect()] else False

      # If target view exists, checking view definitions
      if target_view_exists:

          print("Target view exists. Checking for view definition change...")
          # Get target view description
          target_table_details = (spark.sql(f"DESCRIBE TABLE EXTENDED {target_full_name}"))

          # Get target view text
          target_view_definition = (target_table_details
                                    .filter("col_name = 'View Text'")
                                    .select("data_type")
                                    .collect()[0]["data_type"]
              )
          if not target_view_definition:
              raise ValueError(
                  f"View '{target_full_name}' text could not be found using DESCRIBE TABLE EXTENDED"
              )

          target_view_creation = (target_table_details
                                  .filter("col_name = 'Created Time'")
                                  .select("data_type")
                                  .collect()[0]["data_type"]
                                  )

          # If view definitions are different, set the sync status message to sync
          if target_view_definition != source_view_definition:
              print(
                  f"Target view '{target_full_name}' text '{target_view_definition}' does not match with source view '{source_full_name}' text '{source_view_definition}', marking '{target_full_name}' for drop"
              )
              sync_status.sync_status_description = f"Replace view due to differences in view text"
          else:
              print(
                  f"Views '{source_full_name}' and '{target_full_name}' have the same view text '{source_view_definition}'"
              )
              sync_status.sync_status_description = f"Views have the same view definitions, hence they are in sync"

              sync_status.sync_status_code = "SUCCESS"

              print(sync_status.sync_status_description)

              return sync_status

      # If target view doesn't exist, set sync status to creating
      if not target_view_exists:
          sync_status.sync_status_description = f"Create view since it doesn't exist."
          print(sync_status.sync_status_description)

      # Set the create or replace view statement
      create_or_replace_view_stmt = f"CREATE OR REPLACE VIEW {target_full_name} AS {source_view_definition}"
      
      print(f"Executing '{create_or_replace_view_stmt}'...")
      # Execute the create or replace view statement
      spark.sql(create_or_replace_view_stmt)

      print(f"View {source_full_name} has been created to {target_full_name} with definition {source_view_definition}")

      # Clear all caches first
      spark.sql(f"REFRESH TABLE {target_full_name}")

      sync_status.sync_status_code = "SUCCESS"

  except Exception:
      print(f"View {source_full_name} has been failed to create {target_full_name} with definition {source_view_definition}. With error: {str(traceback.format_exc())}")
      
      sync_status.sync_status_description = str(traceback.format_exc())
      sync_status.sync_status_code = "FAILED"

  return sync_status


def get_user_defined_function_description(spark: SparkSession, source_catalog: str, source_schema: str, source_function: str = None) -> list[FunctionDLL]:
  """
  Get the description of functions from a schema to a list

  Parameters:
    spark: Active SparkSession
    source_catalog: The name of the source catalog.
    source_schema: The name of the source schema
    source_table: The name(s) of the source functions(s) in the given schema. Should be given as a string like "table_1, table_2". All the tables in the given source schema will be used if not given.

  Returns:
    The list of FunctionDLL classes with the function descriptions
  """
  try:

    if not source_schema:
      raise ValueError("Source Schema is empty")

    # List variable for the function description
    function_descriptions = []

    if not source_function:
        # Read all functions in the given schema to DataFrame
        source_functions = (spark
                      .sql(f"SHOW USER FUNCTIONS IN {source_catalog}.{source_schema}")
                      )
        # Get all functions from Hive Metastore for the given hive_metastore schema
        function_names = list(map(lambda r: r.function, source_functions.collect()))

    elif source_function.find(","):
      # The input is a list of functions in a string
      function_names = source_function.split(", ")
    else:
      # The input is a single function
      function_names = [f"{source_function}"]

    # Loop through each function and run the describe command
    for function_name in function_names:
      function_dll = FunctionDLL()
      function_short_name = function_name.split('.')[-1]
      full_function_name = f"{source_catalog}.{source_schema}.{function_short_name}"
      print(f"Getting {full_function_name} descriptions using DESCRIBE EXTENDED command")
      desc_df = (spark.sql(f"DESCRIBE FUNCTION EXTENDED {full_function_name}"))

      # Extract information with regex
      extracted_df = (
        desc_df.withColumn(
          "class",
          F.regexp_extract(F.col("function_desc"), r"^Class:\s+(.*)", 1)
        )
        .withColumn(
          "function_name",
          F.regexp_extract(F.col("function_desc"), r"^Function:\s+(.*)", 1)
        )
        .withColumn(
          "body",
          F.regexp_extract(F.col("function_desc"), r"^Body:\s+(([\S\s]*)*)", 1)
        )
        .withColumn(
          "deterministic",
          F.regexp_extract(F.col("function_desc"), r"^Deterministic:\s+(.*)", 1)
        )
        .withColumn(
          "type",
          F.regexp_extract(F.col("function_desc"), r"^Type:\s+(.*)", 1)
        )
        .withColumn(
          "language",
          F.regexp_extract(F.col("function_desc"), r"^Language:\s+(.*)", 1)
        )
        .withColumn(
          "comment",
          F.regexp_extract(F.col("function_desc"), r"^Comment:\s+(.*)", 1)
        )
      ).replace('', None).select(
        F.first(F.col("class"), ignorenulls=True).alias("class"),
        F.first("function_name", ignorenulls=True).alias("function_name"),
        F.first("body", ignorenulls=True).alias("body"),
        F.first("deterministic", ignorenulls=True).alias("deterministic"),
        F.first("type", ignorenulls=True).alias("type"),
        F.first("language", ignorenulls=True).alias("language"),
        F.first("comment", ignorenulls=True).alias("comment")
      )

      # Collect the extracted information
      extracted_dict = extracted_df.collect()[0].asDict()
      description_list = list(map(lambda r: r.function_desc, desc_df.collect()))

      # Get Input parameters using the description text because it can exists in multiple rows
      inputs = []
      # Get the Input parameter(s)
      input_found = False
      for line in description_list:
        if line.startswith("Input:"):
          input_found = True
          inputs.append(re.search(r"^Input:\s+(.*)", line).group(1))
          continue

        if input_found:
          if line.startswith(" "):
            inputs.append(line.strip())
          else:
            break

      # Get Return values using the description text because it can exists in multiple rows
      returns = []
      # Get the Return type(s)
      returns_found = False
      for line in description_list:
        if line.startswith("Returns:"):
          returns_found = True
          returns.append(re.search(r"^Returns:\s+(.*)", line).group(1))
          continue

        if returns_found:
          if line.startswith(" "):
            returns.append(line.strip())
          else:
            break

      # Fill the FunctionDll object with the correspondig values
      function_dll.body = extracted_dict.get("body")
      function_dll.deterministic = extracted_dict.get("deterministic")
      function_dll.function_class = extracted_dict.get("class")
      # Filter out '()' because it means there are no input
      function_dll.input_parameters = [i for i in inputs if i != '()']
      function_dll.language = extracted_dict.get("language")
      function_dll.name = extracted_dict.get("function_name")
      function_dll.returns = returns
      function_dll.function_type = extracted_dict.get("type")
      function_dll.comment = extracted_dict.get("comment")

      function_descriptions.append(function_dll)

  except (ValueError, U.AnalysisException) as e:
    if isinstance(e, ValueError):
      print(f"ValueError occurred: {e}")
      raise ValueError(f"ValueError occurred: {e}")
    elif isinstance(e, U.AnalysisException):
      print(f"ValueError occurred: {e}")
      raise U.AnalysisException(f"AnalysisException occurred: {e}")

  return function_descriptions


def sync_function(spark: SparkSession, function_description: FunctionDLL, target_catalog: str, target_schema: str, target_function: str = None) -> SyncStatus:
  """
  Sync functions between catalogs using create or replace

  Parameters:
    spark: The active SparkSession
    function_description: List of function(s)' descriptions. Could contain single or multiple function(s)' descriptions
    target_catalog: The name of the target catalog.
    target_schema: The name of the target schema.
    target_function:(optional) The name of the target function. If not given function gets the name of the original function.
  """
  try:
    # Set sync status variable from the SyncStatus object
    sync_status = SyncStatus()

    if not function_description:
      raise ValueError("function_description is empty")
    if not target_catalog:
      raise ValueError("target catalog input string is empty")
    if not target_schema:
      raise ValueError("target schema input string is empty")

    source_function_name = function_description.name
    sync_status.source_object_full_name = source_function_name
    sync_status.source_object_type = "UDF"

    if target_function:
      target_function_name = f"{target_catalog}.{target_schema}.{target_function}"
    else:
      source_function_last_name = source_function_name.split('.')[-1]
      target_function_name = f"{target_catalog}.{target_schema}.{source_function_last_name}"

    sync_status.target_object_full_name = target_function_name

    print(f"Syncing function '{source_function_name}' to '{target_function_name}'")

    # Generate create or replace function statement
    if function_description.function_class:
      # Generate external function statement
      stmt = f"CREATE OR REPLACE FUNCTION {target_function_name} AS '{function_description.function_class}'"
    else:
      # Fix comments in input parameters:
      inputs = [p.replace(" '", " COMMENT '", 1) for p in function_description.input_parameters]
      
      stmt = f"CREATE OR REPLACE FUNCTION {target_function_name}({', '.join(inputs)})"

      if function_description.function_type == "TABLE":
        stmt += f" RETURNS TABLE ({', '.join(function_description.returns)})"
      else:
        stmt += f" RETURNS {function_description.returns[0]}"

      if function_description.comment:
        stmt += f" COMMENT '{function_description.comment}'"

      if function_description.deterministic:
        if function_description.deterministic.lower() == 'true':
          stmt += " DETERMINISTIC"
        else:
          stmt += " NOT DETERMINISTIC"

      if function_description.language and function_description.language.lower() == "python":
        stmt += " LANGUAGE PYTHON"
        stmt += f""" AS $$
          {function_description.body}
        $$
        """
      else:
        stmt += f" RETURN {function_description.body}"

    print(f"Executing '{stmt}'...")
    # Execute the create or replace function statement
    spark.sql(stmt)

    print(f"Function {source_function_name} has been created to {target_function_name}")
          
    sync_status.sync_status_code = "SUCCESS"

  except Exception:
    print(f"Function {source_function_name} has been failed to create {target_function_name}. With error: {str(traceback.format_exc())}")
    
    sync_status.sync_status_description = str(traceback.format_exc())
    sync_status.sync_status_code = "FAILED"

  return sync_status


def replace_hms_managed_table_to_hms_external(spark: SparkSession, dbutils: DBUtils, table_details: dict, target_catalog: str, target_schema: str, temporary_table_suffix: str, target_location: str) -> SyncStatus:
  """
  Replace hive_metastore MANAGED Table(s) to hive_metastore EXTERNAL Table(s) with data movement using the DEEP CLONE command.

  Parameters:
    spark: Active SparkSession
    dbutils: Databricks Utilities
    tables_descriptions: The list of dictionaries of the mounted tables' descriptions. Including 'Location', 'Catalog', 'Database', 'Table', 'Type', 'Provider', 'Comment', 'Table Properties'.
    target_catalog: The name of the target catalog
    target_schema: The name of the target schema
    temporary table suffix: The suffix of the temporary table. This table will be deleted after the process.
  Returns:
    A SyncStatus object that contains the sync status of the table
  
  """
  try:

    # Set the sync_status object
    sync_status = SyncStatus()

    if not table_details:
        raise ValueError("table_details input list is empty")
    elif not target_catalog:
        raise ValueError("target_catalog input string is empty")
    elif not target_location:
        raise ValueError("target_location input string is empty")

    # Set hive_metastore variables
    hms_catalog = table_details["Catalog"]
    sync_status.source_catalog_name = hms_catalog
    hms_schema = table_details["Database"]
    sync_status.source_schema_name = hms_schema
    hms_table = table_details["Table"]
    sync_status.requested_object_name = hms_table
    hms_table_provider = table_details["Provider"]
    sync_status.source_table_format = hms_table_provider
    hms_table_type = table_details["Type"]
    sync_status.source_object_type = hms_table_type
    
    sync_status.target_catalog_name = target_catalog
    target_schema = target_schema if target_schema else hms_schema
    sync_status.target_schema_name = target_schema
    target_table = f"{hms_table}_{temporary_table_suffix}"

    # Set HMS table full name
    hms_full_name = f"{hms_catalog}.{hms_schema}.{hms_table}"
    sync_status.source_object_full_name = hms_full_name
    # Set target table full name
    target_full_name = f"{target_catalog}.{target_schema}.{target_table}"
    sync_status.target_object_full_name = target_full_name

    # Clear all caches first
    spark.sql(f"REFRESH TABLE {hms_full_name}")

    # Set DEEP CLONE COMMAND
    deep_clone_statement = f"""
                        CREATE OR REPLACE TABLE {target_full_name}
                        DEEP CLONE {hms_full_name}
                        """

    print("Migrating with CLONE ...")
    
    print(f"Migrating {hms_table_type} {hms_full_name} table to External {target_full_name} in location {target_location} using DEEP CLONE")
    deep_clone_statement += f" LOCATION '{target_location}'"
    
    sync_status.sync_status_description = f"External temporary table has been created using DEEP CLONE"

    print(f"Executing '{deep_clone_statement}'")
    spark.sql(deep_clone_statement)

    # Check the match of the tables
    check_equality_of_hms_uc_table(spark, hms_schema, hms_table, target_catalog, target_schema, target_table)

    drop_statement = f"DROP TABLE IF EXISTS {hms_full_name}"
    print(f"Executing '{drop_statement}'")
    spark.sql(drop_statement)

    create_statement = f"CREATE TABLE {hms_full_name} USING DELTA LOCATION '{target_location}'"
    print(f"Executing '{create_statement}'")
    spark.sql(create_statement)

    # Drop the temporary table
    temporary_drop_statement = f"DROP TABLE {target_full_name}"
    print(f"Executing '{temporary_drop_statement}'")
    spark.sql(temporary_drop_statement)

    sync_status.sync_status_description = f"External temporary has been switched with original table"

    sync_status.sync_status_code = "SUCCESS"

    print(sync_status.sync_status_description)
  
  except Exception:
      sync_status.sync_status_code = "FAILED"
      sync_status.sync_status_description = str(traceback.format_exc())
      print(str(traceback.format_exc()))
  return sync_status