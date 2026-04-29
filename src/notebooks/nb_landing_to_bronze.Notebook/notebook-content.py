# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {}
# META   }
# META }

# MARKDOWN ********************

# # Brazilian Fish Trade Balance Analysis — Landing to Bronze
# 
# ㅤ
# 
# > ㅤ\
# > This project uses the **ComexStat API** from Brazil's Ministry of Development, Industry, Trade and Services (MDIC), which provides data on Brazilian foreign trade.\
# > ㅤ\
# > Raw `.parquet` files stored in the Landing zone are loaded into the **Bronze** layer of the Lakehouse as Delta tables, preserving the original data while adding control columns.\
# > ㅤ
# 
# ㅤ
# 
# **Notebook:** nb_landing_to_bronze.ipynb
# 
# **Description:** This Notebook is responsible for loading `.parquet` files from the Landing zone into the **Bronze** layer of the `lh_fish_trade` Lakehouse, creating or updating the Delta tables `bronze_trades`, `bronze_cities`, `bronze_countries`, `bronze_cpi`, and `bronze_uf`. Each load's metadata is registered in the `bronze_meta_table`.

# MARKDOWN ********************

# ## Imports

# CELL ********************

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *
import notebookutils
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Parameters

# CELL ********************

lakehouse_name          = 'lh_fish_trade'
lakehouses              = notebookutils.lakehouse.list()
lakehouse               = next((item for item in lakehouses if item.get('displayName') == lakehouse_name), '')

if not lakehouse:
    raise ValueError(f'| ERROR | Lakehouse [{lakehouse_name}] not found')

lakehouse_path          = lakehouse.get('properties').get('abfsPath')
landing_files_path      = f'{lakehouse_path}/Files/Landing'
landing_meta_table_path = f'{lakehouse_path}/Tables/metadata/landing_meta_table'
bronze_meta_table_path  = f'{lakehouse_path}/Tables/metadata/bronze_meta_table'
bronze_table_path       = f'{lakehouse_path}/Tables/bronze' # Data as-is

print(f'| INFO | Files path: {landing_files_path}')
print(f'| INFO | Landing Metatable path: {landing_meta_table_path}')
print(f'| INFO | Bronze Metatable path: {bronze_meta_table_path}')
print(f'| INFO | Bronze table path: {bronze_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Loading auxiliary tables

# CELL ********************

aux_files_obj = notebookutils.fs.ls(f'{landing_files_path}/aux')

# Iter for each file found in aux folder
for file in aux_files_obj:

    source_path = file.path

    # Keep only suffix of the aux file (e.g. 'aux_cities.parquet' turn 'cities')
    # And concat with preffix 'bronze_'
    table_suffix = re.search(r"([^_.]+)\.parquet$", file.name).group(1)
    table_name = f'bronze_{table_suffix}'

    print(f'| INFO | Loading auxiliary table: {table_name}')

    try:

        # Read the aux file and add a datetime column
        df_aux = spark.read.parquet(source_path)
        df_aux = df_aux.withColumn('loaded_at', F.lit(datetime.now()))

        rows = df_aux.count()

        # Write in overwrite mode to delta table at bronze schema
        df_aux.write.format('delta') \
            .mode('overwrite') \
            .option('overwriteSchema', 'true') \
            .save(f'{bronze_table_path}/{table_name}')

        print(f'| SUCCESS | Auxiliary table "{table_name}" loaded successfully. Rows: {rows}')

    except Exception as e:
        print(f'| ERROR | Failed to load auxiliary table "{table_name}": {e}')
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Prepare, create and load schemas and tables

# CELL ********************

table_name = f'bronze_trades'

# Creating bronze meta schema and meta table
bronze_meta_schema = StructType([
    StructField('flow',         StringType(),       False),
    StructField('date_from',    DateType(),         False),
    StructField('date_to',      DateType(),         False),
    StructField('file_path',    StringType(),       False),
    StructField('loaded_at',    TimestampType(),    True),
])

try:
    spark.createDataFrame([], bronze_meta_schema) \
        .write.format('delta') \
        .mode('ignore') \
        .save(bronze_meta_table_path)
    print('| SUCCESS | Bronze metatable created/verified successfully')
    
except Exception as e:
    print(f'| ERROR | Failed to create bronze metatable: {e}')
    raise

# Creating bronze schemas and tables for each flow
bronze_schema = StructType([
    StructField('year',         StringType(),       True),
    StructField('monthNumber',  StringType(),       True),
    StructField('state',        StringType(),       True),
    StructField('noMunMinsgUF', StringType(),       True),
    StructField('country',      StringType(),       True),
    StructField('heading',      StringType(),       True),
    StructField('headingCode',  StringType(),       True),
    StructField('metricFOB',    StringType(),       True),
    StructField('metricKG',     StringType(),       True),
    # Control columns
    StructField("flow",         StringType(),       False),
    StructField("date_from",    DateType(),         False),
    StructField("date_to",      DateType(),         False),
    StructField("file_path",    StringType(),       False), # Landing path
    StructField("ingested_at",  TimestampType(),    False), # Extraction to landing timestamp
    StructField("loaded_at",    TimestampType(),    False), # Loading to bronze timestamp
])

try:
    spark.createDataFrame([], bronze_schema) \
        .write.format('delta') \
        .mode('ignore') \
        .save(f'{bronze_table_path}/{table_name}')
    print(f'| SUCCESS | Table [{table_name}] created/verified successfully')
    
except Exception as e:
    print(f'| ERROR | Failed to create table [{table_name}]: {e}')
    raise

# Load landing_meta and bronze_meta
df_landing_meta = spark.read.format('delta').load(landing_meta_table_path)
df_bronze_meta = spark.read.format('delta').load(bronze_meta_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Find candidates to load

# CELL ********************

# Identify candidate rows for ingestion (newers or updated)
df_bronze_latest_date_to = df_bronze_meta.groupBy('flow').agg(
    F.max(F.col('date_to')).alias('max_date_to')
)

# Identify candidate files to ingestion
df_bronze_candidates = (
    df_landing_meta
    .join(
        df_bronze_latest_date_to,
        on='flow',
        how='left',
    )
    .filter(
        (F.col('max_date_to').isNull()) |           # Full ingestion
        (F.col('date_from') > F.col('max_date_to')) # Incremental ingestion
    )
    .select(
        df_landing_meta.flow,
        df_landing_meta.date_from,
        df_landing_meta.date_to,
        df_landing_meta.target_path,
    )
    .persist()
)

num_candidates = df_bronze_candidates.count()
print(f"Files to process: {num_candidates}")
display(df_bronze_candidates.orderBy("flow", "date_to"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Process the load

# CELL ********************

if num_candidates == 0:
    print(f'| INFO | Nothing to load. Skipping...')
else:
    # 1) Collect only the PATHS (small footprint) – the heavy work is not here
    candidate_paths = [r["target_path"] for r in df_bronze_candidates.select("target_path").toLocalIterator()]

    # 2) Read ALL candidates at once (single Spark job), with minimal parsing.
    df_all = (
        spark.read
            .format('parquet')
            .load(candidate_paths)
    )

    df_all = (
        df_all
        .withColumn("file_path_raw", F.input_file_name())
        .withColumn('file_path_clean', F.regexp_extract(F.col('file_path_raw'), r'(abfss://.*?\.parquet)(?:/|$)', 1))
    )

    # Join to attach control metadata columns from landing_meta
    df_all = (
        df_all
        .join(
            df_bronze_candidates,
            df_all.file_path_clean == df_bronze_candidates.target_path,
            how='left',
        )
        .drop('target_path')
    )

    loaded_at = datetime.now()

    df_all = df_all \
        .withColumn('file_path', F.col('file_path_clean')) \
        .withColumn('loaded_at', F.lit(loaded_at))

    # Final selection in order of the schema
    df_all = df_all.select(
        'year', 'monthNumber', 'state', 'noMunMinsgUF', 'country', 'heading', 'headingCode', 'metricFOB', 'metricKG',
        'flow', 'date_from', 'date_to', 'file_path', 'ingested_at', 'loaded_at'
    )

    # Single merge using delta library
    target = DeltaTable.forPath(spark, f'{bronze_table_path}/{table_name}')
    (
        target.alias('target')
        .merge(
            df_all.alias('source'),
            'target.file_path = source.file_path'
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Write bronze meta
    df_to_meta = (
        df_bronze_candidates
        .select(
            F.col('flow'),
            F.col('date_from'),
            F.col('date_to'),
            F.col('target_path').alias('file_path')
        )
        .withColumn('loaded_at', F.lit(loaded_at))
    )
    df_to_meta.write.format('delta').mode('append').save(bronze_meta_table_path)

    print(f"| SUCCESS | Processed files to Bronze: {num_candidates}")

    # Cleanup
    df_bronze_candidates.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
