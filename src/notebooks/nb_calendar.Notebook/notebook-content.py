# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
gold_table_path         = f'{lakehouse_path}/Tables/gold/dim_calendar'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Date Parameters

# CELL ********************

start_date = '2006-01-01'
end_date = '2026-12-31'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(f"""
with dates as (
    select
        explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}')
            )
        ) as d
)
select cast(d as date)              as data
     , year(d)                      as ano
     , month(d)                     as mes_numero
     , date_format(d, 'MMMM')       as mes_nome
     , date_format(d, 'MMM')        as mes_nome_abreviado
     , date_format(d, 'MMM/yy')     as mes_ano_nome
     , (year(d) * 100) + month(d)   as mes_ano_numero
from dates
""")

df.write.format('delta').mode('overwrite').save(gold_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
