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

# # Brazilian Fish Trade Balance Analysis — Bronze to Silver
# 
# ㅤ
# 
# > ㅤ\
# > This project uses the **ComexStat API** from Brazil's Ministry of Development, Industry, Trade and Services (MDIC), which provides data on Brazilian foreign trade.\
# > ㅤ\
# > The **Silver** layer consolidates raw Bronze data by applying cleaning, standardization, and enrichment — ensuring referential integrity and data quality prior to dimensional modeling.\
# > ㅤ
# 
# ㅤ
# 
# **Notebook:** nb_bronze_to_silver.ipynb
# 
# **Description:** This Notebook is responsible for transforming data from the **Bronze** layer into the **Silver** layer of the `lh_fish_trade` Lakehouse, performing cleaning, nomenclature standardization, type casting, and enrichment with auxiliary tables. The output is the validated Delta table `silver_trades` and dimension tables, ready for modeling.

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
bronze_table_path       = f'{lakehouse_path}/Tables/bronze'
silver_table_path       = f'{lakehouse_path}/Tables/silver'

print(f'| INFO | Bronze metatable path: {meta_table_path}/bronze_meta_table')
print(f'| INFO | Bronze table path: {bronze_table_path}')
print(f'\n| INFO | Silver metatable path: {meta_table_path}/silver_meta_table')
print(f'| INFO | Silver table path: {silver_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup — Silver Schemas and Tables

# CELL ********************

# Define schemas to silver stage with nomenclature rules (snake_case)
silver_cities_schema = StructType([
    StructField('municipio_id',     StringType(),       False),
    StructField('municipio_nome',   StringType(),       False),
    StructField('estado_nome',      StringType(),       False),
    StructField('uf',               StringType(),       False),
    StructField('municipio_uf',     StringType(),       False),
    StructField('regiao',           StringType(),       False),
    # Control columns
    StructField('ingested_at',      TimestampType(),    True),
    StructField('loaded_at',        TimestampType(),    True),
    StructField('processed_at',     TimestampType(),    True),
])

silver_countries_schema = StructType([
    StructField('pais_id',      StringType(),       False),
    StructField('pais_nome',    StringType(),       False),
    # Control columns
    StructField('ingested_at',  TimestampType(),    True),
    StructField('loaded_at',    TimestampType(),    True),
    StructField('processed_at', TimestampType(),    True),
])

silver_cpi_schema = StructType([
    StructField('cpi_data',     DateType(),         False),
    StructField('cpi_valor',    DecimalType(20, 2), False),
    # Control columns
    StructField('ingested_at',  TimestampType(),    True),
    StructField('loaded_at',    TimestampType(),    True),
    StructField('processed_at', TimestampType(),    True),
])

silver_trades_schema = StructType([
    StructField('comercio_data',    DateType(),         False),
    StructField('municipio_id',     StringType(),       False),
    StructField('pais_id',          StringType(),       False),
    StructField('categoria_id',     StringType(),       False),
    StructField('valor_nominal',    DecimalType(20, 2), False),
    StructField('peso_liquido',     DecimalType(20, 2), False),
    StructField('tipo_comercio',    StringType(),       False),
    # Control columns
    StructField('date_from',        DateType(),         False),
    StructField('date_to',          DateType(),         False),
    StructField('file_path',        StringType(),       False),
    StructField('ingested_at',      TimestampType(),    True),
    StructField('loaded_at',        TimestampType(),    True),
    StructField('processed_at',     TimestampType(),    True),
])

silver_meta_schema = StructType([
    StructField('flow',         StringType(),       False),
    StructField('date_from',    DateType(),         False),
    StructField('date_to',      DateType(),         False),
    StructField('file_path',    StringType(),       False),
    StructField('processed_at', TimestampType(),    True),
])

silver_schemas = {
    'silver_cities':        silver_cities_schema,
    'silver_countries':     silver_countries_schema,
    'silver_cpi':           silver_cpi_schema,
    'silver_trades':        silver_trades_schema,
    'silver_meta_table':    silver_meta_schema,
}

# Create silver tables (if not exists)
for table_name, schema in silver_schemas.items():

    if not table_name.endswith('meta_table'):
        try:
            spark.createDataFrame([], schema) \
                .write.format('delta') \
                .mode('ignore') \
                .save(f'{silver_table_path}/{table_name}')
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
            print('| SUCCESS | Silver metatable created/verified successfully')
    
        except Exception as e:
            print(f'| ERROR | Failed to create silver metatable: {e}')
            raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read bronze tables ans silver_meta_table

# CELL ********************

bronze_tables = ['bronze_cities', 'bronze_countries', 'bronze_cpi', 'bronze_uf', 'bronze_trades', 'bronze_meta_table', 'silver_meta_table']

# Iter for each table name and read the bronze table
dfs = {}
for table_name in bronze_tables:

    if not table_name.endswith('meta_table'):
        dfs[table_name] = spark.read.format('delta').load(f'{bronze_table_path}/{table_name}')

    else:
        dfs[table_name] = spark.read.format('delta').load(f'{meta_table_path}/{table_name}')

df_bronze_cities = dfs['bronze_cities']
df_bronze_countries = dfs['bronze_countries']
df_bronze_cpi = dfs['bronze_cpi']
df_bronze_uf = dfs['bronze_uf']
df_bronze_trades = dfs['bronze_trades']
df_bronze_meta_table = dfs['bronze_meta_table']
df_silver_meta_table = dfs['silver_meta_table']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Support DataFrames — Regions and Categories

# CELL ********************

df_regions = spark.createDataFrame([
    ('AC', 'Norte'),
    ('AM', 'Norte'),
    ('AP', 'Norte'),
    ('PA', 'Norte'),
    ('RO', 'Norte'),
    ('RR', 'Norte'),
    ('TO', 'Norte'),
    ('AL', 'Nordeste'),
    ('BA', 'Nordeste'),
    ('CE', 'Nordeste'),
    ('MA', 'Nordeste'),
    ('PB', 'Nordeste'),
    ('PE', 'Nordeste'),
    ('PI', 'Nordeste'),
    ('RN', 'Nordeste'),
    ('SE', 'Nordeste'),
    ('DF', 'Centro-Oeste'),
    ('GO', 'Centro-Oeste'),
    ('MT', 'Centro-Oeste'),
    ('MS', 'Centro-Oeste'),
    ('ES', 'Sudeste'),
    ('MG', 'Sudeste'),
    ('RJ', 'Sudeste'),
    ('SP', 'Sudeste'),
    ('PR', 'Sul'),
    ('RS', 'Sul'),
    ('SC', 'Sul'),
], ['uf', 'regiao'])

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

# ## Transform — Auxiliary Tables (Cities, Countries, CPI)

# CELL ********************

df_cities_transformed = (
	df_bronze_cities.alias('ct')
	.withColumn('uf', F.regexp_extract(F.col('ct.text'), r'[A-Z]{2}$', 0))
	.join(
		df_bronze_uf.alias('uf'),
		on='uf',
		how='left',
	)
	.join(
		df_regions.alias('rg'),
		on='uf',
		how='left',
	)	
	.select(	
		F.col('ct.id')																				.cast(StringType())		.alias('municipio_id'),
		F.col('ct.noMunMin')																		.cast(StringType())		.alias('municipio_nome'),
		F.col('uf.text')																			.cast(StringType())		.alias('estado_nome'),
		F.col('uf')																					.cast(StringType())		.alias('uf'),
		F.col('ct.text')																			.cast(StringType())		.alias('municipio_uf'),
		F.when(F.col('rg.regiao').isNull(), F.lit('Não se aplica')).otherwise(F.col('rg.regiao'))	.cast(StringType())		.alias('regiao'),
		F.col('ct.ingested_at')																		.cast(TimestampType())	.alias('ingested_at'),
		F.col('ct.loaded_at')																		.cast(TimestampType())	.alias('loaded_at'),
		F.lit(datetime.now())																								.alias('processed_at'),
	)
)

df_countries_transformed =(
	df_bronze_countries
	.select(
		F.col('id')				.cast(StringType())		.alias('pais_id'),
		F.col('text')			.cast(StringType())		.alias('pais_nome'),
		F.col('ingested_at')	.cast(TimestampType())	.alias('ingested_at'),
		F.col('loaded_at')		.cast(TimestampType())	.alias('loaded_at'),
		F.lit(datetime.now())							.alias('processed_at'),
	)
)

df_cpi_transformed = (
	df_bronze_cpi
	.select(
		F.to_date(
			F.concat(

				# If year doesn't exist, insert '1990'
				F.coalesce(F.col('year'), F.lit('1990')), F.lit('/'),

				# The column period is the month with format ('M1' to 'M12')
				# With F.regexp_replace, everything that is not a number is replaced by a empty string
				# With F.lpad, we garantee two characters, so the one number months are showed with a left zero
				F.coalesce(F.lpad(F.regexp_replace(F.col('period'), '[^0-9]', ''), 2, '0'), F.lit('01')), F.lit('/'),
				F.lit('01')
			),
			'yyyy/MM/dd',
		)																													.alias('cpi_data'),
		F.when(F.col('value') == '-', F.lit(0)).otherwise(F.coalesce(F.col('value'), F.lit(0)))	.cast(DecimalType(20, 2))	.alias('cpi_valor'),
		F.col('ingested_at')																	.cast(TimestampType())		.alias('ingested_at'),
		F.col('loaded_at')																		.cast(TimestampType())		.alias('loaded_at'),
		F.lit(datetime.now())																								.alias('processed_at'),
	)
)

print(f'| SUCCESS | DataFrame df_cities_transformed created successfully. Columns: {len(df_cities_transformed.columns)} | Rows: {df_cities_transformed.count()}')
print(f'| SUCCESS | DataFrame df_countries_transformed created successfully. Columns: {len(df_countries_transformed.columns)} | Rows: {df_countries_transformed.count()}')
print(f'| SUCCESS | DataFrame df_cpi_transformed created successfully. Columns: {len(df_cpi_transformed.columns)} | Rows: {df_cpi_transformed.count()}\n')

silver_aux_tables = {
	'silver_cities': df_cities_transformed,
	'silver_countries': df_countries_transformed,
	'silver_cpi': df_cpi_transformed
}

# Aux tables (cities, countries, cpi) are static — always overwrite
for table_name, df_aux in silver_aux_tables.items():
		
	print(f'| INFO | Loading auxiliary table: {table_name}')

	try:

		rows = df_aux.count()

		# Write in overwrite mode to delta table at silver schema
		df_aux.write.format('delta') \
			.mode('overwrite') \
			.option('overwriteSchema', 'true') \
			.save(f'{silver_table_path}/{table_name}')

		print(f'| SUCCESS | Auxiliary table "{table_name}" processed successfully. Rows: {rows}')

	except Exception as e:
		print(f'| ERROR | Failed to load auxiliary table "{table_name}": {e}')
		raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Incremental Check — Silver Candidates

# CELL ********************

# Calculate latest date_to by flow (import/export) at silver_meta_table
df_silver_latest_date_to = df_silver_meta_table.groupBy('flow').agg(
    F.max(F.col('date_to')).alias('max_date_to')
)

# Identify candidate rows for ingestion (newers or updated)
# Left join + filter identifies only files not yet processed in silver
df_silver_candidates = (
    df_bronze_trades.alias('b')
    .join(
        df_silver_latest_date_to.alias('s'),
        on='flow',
        how='left',
    )
    .filter(
        (F.col('max_date_to').isNull()) |           # Full ingestion
        (F.col('date_from') > F.col('max_date_to')) # Incremental ingestion
    )
    .select(
        *[F.col(f'b.{col}') for col in df_bronze_trades.columns],
    )
    .persist()
)

# Count candidates
rows = df_silver_candidates.count()
print(f'Rows to process from Bronze to Silver: {rows}.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transform — Trades

# CELL ********************

if rows == 0:
    print(f'| INFO | Nothing to process. Skipping...')
else:
	df_trades_transformed = (
		df_silver_candidates.alias('trades')

		# Create quality flags
		.withColumn(
			'is_valid_value',
			F.when(F.col('metricFOB').cast(DecimalType(20,0)) > 0, True).otherwise(False)
		)
		.withColumn(
			'is_valid_weight',
			F.when(F.col('metricKG').cast(DecimalType(20,0)) > 0, True).otherwise(False)
		)

		# Filter by quality flags
		.filter(
			(F.col('is_valid_value')) & (F.col('is_valid_weight'))
		)

		# Normalize city and country name columns (lowercase letters only) to perform the join
		.withColumn(
			'cidade_nome_norm',
			F.regexp_replace(
				F.translate(
					F.lower(F.col('noMunMinsgUF')),
					'áàãâäéèêëíìîïóòõôöúùûüç',
					'aaaaaeeeeiiiiooooouuuuc'
				),
				'[^a-z]',
				''
			)
		)
		.withColumn(
			'pais_nome_norm',
			F.regexp_replace(
				F.translate(
					F.lower(F.col('country')),
					'áàãâäéèêëíìîïóòõôöúùûüç',
					'aaaaaeeeeiiiiooooouuuuc'
				),
				'[^a-z]',
				''
			)
		)

		# Join with cities and countries tables to catch ids
		.join(
			df_cities_transformed.alias('cities'),
			F.col('cidade_nome_norm') == F.regexp_replace(
													F.translate(
														F.lower(
															F.col('cities.municipio_uf')),
															'áàãâäéèêëíìîïóòõôöúùûüç',
															'aaaaaeeeeiiiiooooouuuuc'
														),
														'[^a-z]',
														''
													).alias('municipio_uf'),
			how='left',
		)
		.join(
			df_countries_transformed.alias('countries'),
			F.col('pais_nome_norm') == F.regexp_replace(
													F.translate(
														F.lower(
															F.col('countries.pais_nome')),
															'áàãâäéèêëíìîïóòõôöúùûüç',
															'aaaaaeeeeiiiiooooouuuuc'
														),
														'[^a-z]',
														''
													).alias('pais_nome'),
			how='left',
		)
		.select(
			F.to_date(
				F.concat(
					F.coalesce(F.col('year'), F.lit('1990')), F.lit('/'),
					F.coalesce(F.lpad(F.col('monthNumber'), 2, '0'), F.lit('01')), F.lit('/'),
					F.lit('01'),
				),
				'yyyy/MM/dd'
			).alias('comercio_data'),
			F.coalesce(F.col('cities.municipio_id'), F.lit('N/A'))			.cast(StringType())			.alias('municipio_id'),
			F.coalesce(F.col('countries.pais_id'), F.lit('N/A'))			.cast(StringType())			.alias('pais_id'),
			F.coalesce(F.col('trades.headingCode'), F.lit('0000'))			.cast(StringType())			.alias('categoria_id'),
			F.coalesce(F.col('trades.metricFOB'), F.lit(0))					.cast(DecimalType(20,2))	.alias('valor_nominal'),
			F.coalesce(F.col('trades.metricKG'), F.lit(0))					.cast(DecimalType(20,2))	.alias('peso_liquido'),
			F.when(F.col('trades.flow') == 'import', F.lit('Importação'))
			.when(F.col('trades.flow') == 'export', F.lit('Exportação'))	.cast(StringType())			.alias('tipo_comercio'),
			F.col('trades.date_from')										.cast(DateType())			.alias('date_from'),
			F.col('trades.date_to')											.cast(DateType())			.alias('date_to'),
			F.col('trades.file_path')										.cast(StringType())			.alias('file_path'),
			F.col('trades.ingested_at')										.cast(TimestampType())		.alias('ingested_at'),
			F.col('trades.loaded_at')										.cast(TimestampType())		.alias('loaded_at'),
			F.lit(datetime.now())																		.alias('processed_at'),
		)
	)

	rows_transformed = df_trades_transformed.count()

	print(f'| SUCCESS | DataFrame df_trades_transformed created successfully. Columns: {df_trades_transformed.columns} | Rows: {rows_transformed}')

	# Single merge using delta library
	target = DeltaTable.forPath(spark, f'{silver_table_path}/silver_trades')
	(
        target.alias('target')
        .merge(
            df_trades_transformed.alias('source'),
            'target.file_path = source.file_path'
        )
        .whenNotMatchedInsertAll()
        .execute()
	)

	# Write silver meta
	processed_files = (
		df_silver_candidates
		.select(
			F.col('flow'),
			F.col('date_from'),
			F.col('date_to'),
			F.col('file_path')
		)
		.distinct().collect()
	)

	df_processed = spark.createDataFrame([
		{
			'flow': row['flow'],
			'date_from': row['date_from'],
			'date_to': row['date_to'],
			'file_path': row['file_path'],
			'processed_at': datetime.now()
		} for row in processed_files],
		schema=silver_meta_schema
	)

	df_processed.write.format('delta').mode('append').save(f'{meta_table_path}/silver_meta_table')

	print(f"| SUCCESS | Processed rows to Silver: {rows_transformed}")

    # Cleanup
	df_silver_candidates.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
