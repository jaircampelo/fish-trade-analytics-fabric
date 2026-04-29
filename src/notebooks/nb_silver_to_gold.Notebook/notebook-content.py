# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "48c95fdf-3d2e-a5a4-4c65-65bbf007c394",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Brazilian Fish Trade Balance Analysis — Silver to Gold
# 
# ㅤ
# 
# > ㅤ\
# > This project uses the **ComexStat API** from Brazil's Ministry of Development, Industry, Trade and Services (MDIC), which provides data on Brazilian foreign trade.\
# > ㅤ\
# > The **Gold** layer implements a **Star Schema** dimensional model, structuring data for direct consumption by the Direct Lake Semantic Model in **Power BI**.\
# > ㅤ
# 
# ㅤ
# 
# **Notebook:** nb_silver_to_gold.ipynb
# 
# **Description:** This Notebook is responsible for building the dimensional model in the **Gold** layer of the `lh_fish_trade` Lakehouse, creating the Delta tables `fact_trades`, `dim_cities`, `dim_countries`, and `dim_product_categories` from Silver layer data and seeds defined as DataFrames.

# MARKDOWN ********************

# ## Imports

# CELL ********************

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import *
import notebookutils

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
meta_table_path         = f'{lakehouse_path}/Tables/metadata'
silver_table_path       = f'{lakehouse_path}/Tables/silver'
gold_table_path         = f'{lakehouse_path}/Tables/gold'

print(f'| INFO | Silver metatable path: {meta_table_path}/silver_meta_table')
print(f'| INFO | Silver table path: {silver_table_path}')
print(f'\n| INFO | Gold metatable path: {meta_table_path}/gold_meta_table')
print(f'| INFO | Gold table path: {gold_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup — Gold Schemas and Tables

# CELL ********************

# Define schemas to silver stage with nomenclature rules (snake_case)
dim_cities_schema = StructType([
    StructField('municipio_id',     StringType(),       False),
    StructField('municipio_nome',   StringType(),       False),
    StructField('estado_nome',      StringType(),       False),
    StructField('uf',               StringType(),       False),
    StructField('municipio_uf',     StringType(),       False),
    StructField('regiao',           StringType(),       False),
])

dim_countries_schema = StructType([
    StructField('pais_id',      StringType(),       False),
    StructField('pais_nome',    StringType(),       False),
])

dim_categories_schema = StructType([
    StructField('categoria_id',         StringType(),       False),
    StructField('categoria_nome',       StringType(),       False),
    StructField('categoria_descricao',  StringType(),       False),
])

fact_trades_schema = StructType([
    StructField('comercio_data',    DateType(),         False),
    StructField('municipio_id',     StringType(),       False),
    StructField('pais_id',          StringType(),       False),
    StructField('categoria_id',     StringType(),       False),
    StructField('valor_nominal',    DecimalType(20, 2), False),
    StructField('valor_real',       DecimalType(20, 2), False),
    StructField('peso_liquido',     DecimalType(20, 2), False),
    StructField('tipo_comercio',    StringType(),       False),
    # Control columns
    StructField('date_from',        DateType(),         False),
    StructField('date_to',          DateType(),         False),
    StructField('file_path',        StringType(),       False),
    StructField('created_at',       TimestampType(),    True),
])

gold_meta_schema = StructType([
    StructField('flow',         StringType(),       False),
    StructField('date_from',    DateType(),         False),
    StructField('date_to',      DateType(),         False),
    StructField('file_path',    StringType(),       False),
    StructField('created_at',   TimestampType(),    True),
])

gold_schemas = {
    'dim_cities':       dim_cities_schema,
    'dim_countries':    dim_countries_schema,
    'dim_categories':   dim_categories_schema,
    'fact_trades':      fact_trades_schema,
    'gold_meta_table':  gold_meta_schema,
}

# Create gold tables (if not exists)
for table_name, schema in gold_schemas.items():

    if not table_name.endswith('meta_table'):
        try:
            spark.createDataFrame([], schema) \
                .write.format('delta') \
                .mode('ignore') \
                .save(f'{gold_table_path}/{table_name}')
            print(f'| SUCCESS | {table_name} created/verified successfully')
            
        except Exception as e:
            print(f'| ERROR | Failed to create {table_name}: {e}')
            raise

    else:
        try:
            spark.createDataFrame([], schema) \
                .write.format('delta') \
                .mode('ignore') \
                .save(f'{meta_table_path}/{table_name}')
            print('| SUCCESS | Gold metatable created/verified successfully')
    
        except Exception as e:
            print(f'| ERROR | Failed to create gold metatable: {e}')
            raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read silver tables and gold_meta_table

# CELL ********************

silver_tables = ['silver_cities', 'silver_countries', 'silver_cpi', 'silver_trades', 'silver_meta_table', 'gold_meta_table']

# Iter for each table name and read the silver table
dfs = {}
for table_name in silver_tables:

    if not table_name.endswith('meta_table'):
        dfs[table_name] = spark.read.format('delta').load(f'{silver_table_path}/{table_name}')

    else:
        dfs[table_name] = spark.read.format('delta').load(f'{meta_table_path}/{table_name}')

df_silver_cities        = dfs['silver_cities']
df_silver_countries     = dfs['silver_countries']
df_silver_cpi           = dfs['silver_cpi']
df_silver_trades        = dfs['silver_trades']
df_silver_meta_table    = dfs['silver_meta_table']
df_gold_meta_table      = dfs['gold_meta_table']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create support categories DataFrame

# CELL ********************

df_categories = spark.createDataFrame([
    ('0301', 'Peixes vivos', 'Peixes vivos'),
    ('0302', 'Peixes frescos ou refrigerados', 'Peixes frescos ou refrigerados exceto filés'),
    ('0303', 'Peixes congelados', 'Peixes congelados exceto filés'),
    ('0304', 'Filés de peixes', 'Filés de peixes e outra carne de peixes (mesmo picada), frescos, refrigerados ou congelados'),
    ('0305', 'Peixes secos, salgados ou em salmoura', 'Peixes secos, salgados ou em salmoura, defumados, mesmo cozidos antes ou durante a defumação e farinhas, pós e pellets, de peixe, próprios para alimentação humana'),
    ('0306', 'Crustáceos', 'Crustáceos, mesmo sem casca, vivos, frescos, refrigerados, congelados, secos, salgados ou em salmoura, com casca, cozidos em água ou vapor, mesmo refrigerados, congelados, secos, salgados ou em salmoura e farinhas, pó e pellets de crustáceos'),
    ('0307', 'Moluscos', 'Moluscos, com ou sem concha, vivos, frescos, refrigerados, congelados, secos, salgados ou em salmoura, invertebrados aquáticos, exceto crustáceos e moluscos, vivos, frescos, refrigerados, congelados, secos, salgados ou em salmoura e farinhas, pó e pellets'),
    ('0308', 'Invertebrados aquáticos', 'Invertebrados aquáticos, exceto crustáceos e moluscos, vivos'),
], ['categoria_id', 'categoria_nome', 'categoria_descricao'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load dimensions

# CELL ********************

dimension_tables = {
    'dim_cities': df_silver_cities,
    'dim_countries': df_silver_countries,
    'dim_categories': df_categories,
}

# Aux tables (cities, countries, categories) are static — always overwrite
for table_name, df in dimension_tables.items():
		
	print(f'| INFO | Loading dimension table: {table_name}')

	try:

		rows = df.count()

		# Write in overwrite mode to delta table at gold schema
		df.write.format('delta') \
			.mode('overwrite') \
			.option('overwriteSchema', 'true') \
			.save(f'{gold_table_path}/{table_name}')

		print(f'| SUCCESS | Dimension table "{table_name}" created successfully. Rows: {rows}')

	except Exception as e:
		print(f'| ERROR | Failed to create dimension table "{table_name}": {e}')
		raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Find Gold candidates to incremental load

# CELL ********************

# Calculate latest date_to by flow (import/export) at gold_meta_table
df_gold_latest_date_to = df_gold_meta_table.groupBy('flow').agg(
    F.max(F.col('date_to')).alias('max_date_to')
)

# Identify candidate rows for ingestion (newers or updated)
# Left join + filter identifies only files not yet created in gold
df_gold_candidates = (
    df_silver_trades.alias('s')
    .join(
        df_gold_latest_date_to.alias('g'),
        F.col('s.tipo_comercio') == F.col('g.flow'),
        how='left',
    )
    .filter(
        (F.col('max_date_to').isNull()) |           # Full ingestion
        (F.col('date_from') > F.col('max_date_to')) # Incremental ingestion
    )
    .select(
        *[F.col(f's.{col}') for col in df_silver_trades.columns],
    )
    .persist()
)

# Count candidates
rows = df_gold_candidates.count()
print(f'Rows to process from Silver to Gold: {rows}.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Apply bussiness rules to fact_trades

# CELL ********************

if rows == 0:
    print(f'| INFO | Nothing to process. Skipping...')
else:

    df_max_cpi = (
        df_silver_cpi
        .sort(F.col('cpi_data'), ascending=False)
        .select(F.col('cpi_valor').alias('max_cpi_valor'))
        .limit(1)
    )

    df_fact_trades_transformed = (
        df_gold_candidates.alias('trades')
        .join(
            df_silver_cpi.alias('cpi'),
            F.col('trades.comercio_data') == F.col('cpi.cpi_data'),
            how='left',
        )
        .crossJoin(df_max_cpi)
        .withColumn(
            'valor_real',
            (F.col('valor_nominal') * (F.try_divide(F.col('max_cpi_valor'), F.col('cpi_valor'))))
        )
        .select(
            F.col('comercio_data'),
            F.col('municipio_id'),
            F.col('pais_id'),
            F.col('categoria_id'),
            F.col('valor_nominal'),
            F.col('valor_real').cast(DecimalType(20, 2)).alias('valor_real'),
            F.col('peso_liquido'),
            F.col('tipo_comercio'),
            F.col('date_from'),
            F.col('date_to'),
            F.col('file_path'),
            F.lit(datetime.now()).alias('created_at')
        )
    )

    rows_transformed = df_fact_trades_transformed.count()

    print(f'| SUCCESS | DataFrame df_fact_trades_final created successfully. Columns: {df_fact_trades_transformed.columns} | Rows: {rows_transformed}')

    # Single merge using delta library
    target = DeltaTable.forPath(spark, f'{gold_table_path}/fact_trades')
    (
        target.alias('target')
        .merge(
            df_fact_trades_transformed.alias('source'),
            'target.file_path = source.file_path'
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Write gold meta
    processed_files = (
    	df_fact_trades_transformed
    	.select(
    		F.col('tipo_comercio').alias('flow'),
    		F.col('date_from'),
    		F.col('date_to'),
    		F.col('file_path'),
            F.col('created_at'),
    	)
    	.distinct().collect()
    )

    df_processed = spark.createDataFrame([
    	{
    		'flow': row['flow'],
    		'date_from': row['date_from'],
    		'date_to': row['date_to'],
    		'file_path': row['file_path'],
    		'created_at': row['created_at']
    	} for row in processed_files],
    	schema=gold_meta_schema
    )

    df_processed.write.format('delta').mode('append').save(f'{meta_table_path}/gold_meta_table')

    print(f"| SUCCESS | Processed rows to Gold: {rows_transformed}")

    # Cleanup
    df_gold_candidates.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
