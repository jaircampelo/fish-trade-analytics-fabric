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

# ## Libs

# CELL ********************

# imports
import requests
from typing import Literal
from datetime import date, datetime
from time import sleep
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

# Fabric
lakehouse_name = 'lh_fish_trade'
lakehouses = notebookutils.lakehouse.list()
lakehouse = next((item for item in lakehouses if item.get('displayName') == lakehouse_name), '')
lakehouse_path = lakehouse.get('properties').get('abfsPath')
landing_files_path = f'{lakehouse_path}/Files/Landing'
landing_meta_table_path = f'{lakehouse_path}/Tables/metadata/landing_meta_table'
key_vault = "https://jaircampelo-kv.vault.azure.net/"

# API
BASE_COMEXSTAT_URL = 'https://api-comexstat.mdic.gov.br'
BASE_CPI_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
start_year = 2006
final_year = date.today().year

print(f'Files path: {landing_files_path}')
print(f'Metatable path: {landing_meta_table_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating landing_meta_table

# CELL ********************

# Creating schema
landing_meta_schema = StructType([
    StructField('flow',         StringType(),       False),
    StructField('date_from',    DateType(),         False),
    StructField('date_to',      DateType(),         False),
    StructField('target_path',  StringType(),       True),
    StructField('ingested_at',    TimestampType(),    True),
])

# Creating delta table
try:
    spark.createDataFrame([], landing_meta_schema) \
        .write.format('delta') \
        .mode('ignore') \
        .save(landing_meta_table_path)
    print('| SUCCESS | Landing metatable created/verified successfully.')
    
except Exception as e:
    print(f'| ERROR | Failed to create landing metatable: {e}')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Getting heading codes from COMEX STAT API

# CELL ********************

# Base API URL
heading_url = f'{BASE_COMEXSTAT_URL}/cities/filters/heading'

# Requesting data
print(f'| INFO | Request heading data from {heading_url}')
resp = requests.get(heading_url, params={'language': 'pt'})

if not resp.ok:
    try:
        error_msg = resp.json().get('error', {}).get('message', resp.text)
    except Exception:
        error_msg = resp.text
    raise requests.HTTPError(f'{resp.status_code}: {error_msg}', response=resp)
else:
    try:
        headings = resp.json().get('data')[0]
        
        if not headings:
            raise ValueError('API returned empty headings list.')

        # Extract only 0301 to 0308 codes
        heading_codes = [
            heading['id']
            for heading in headings
            if heading['id'].startswith('03') and not heading['id'].endswith('09')
        ]

        if not heading_codes:
            raise ValueError('No fish-related SH4 codes (03xx) found in API response.')

        print(f'| SUCCESS | Heading codes extracted successfully: {heading_codes}')

    except Exception as e:
        print(f'| ERROR | Failed to extract heading codes from {heading_url}: {e}')
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Getting API last date update

# CELL ********************

# Base API URL
date_url = f'{BASE_COMEXSTAT_URL}/cities/dates/updated'

# Requesting data
print(f'| INFO | Request last date update from {date_url}')
resp = requests.get(date_url)

if not resp.ok:
    try:
        error_msg = resp.json().get('error', {}).get('message', resp.text)
    except Exception:
        error_msg = resp.text
    raise requests.HTTPError(f'{resp.status_code}: {error_msg}', response=resp)
else:
    try:
        data = resp.json().get('data')

        if not data:
            raise ValueError('API returned empty last date.')

        # Casting month and year data to date format
        api_last_date =  date(int(data['year']), int(data['monthNumber']), 1)

        print(f'| SUCCESS | Latest available date from API: {api_last_date}')

    except Exception as e:
        print(f'| ERROR | Failed to extract last data from {date_url}: {e}')
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Define functions

# CELL ********************

# Requests trade data from COMEX STAT /cities endpoint for a given flow and period.
def request_comexstat(
        year_interval: dict[str, str],
        filters: list[dict[str, list]],
        metrics: list[str],
        details: list[str],
        flow: Literal['import', 'export'],
        *,
        month_detail: bool = True,
):
    """
    Returns a DataFrame from the API request.

    Args:
        year_interval (dict[str, str]): a tuple with the year range of the request.
        filters (list[dict[str, list]]): a dictionary with the filter type and its values in a list.
        metrics (list[str]): defines the metrics to be evaluated.
        details (list[str]): defines the categories by which the metrics will be broken down (determines the granularity of the request).
        flow (Literal['import', 'export']): defines which type of data will be returned.
        month_detail (bool): defines the time granularity. If no value is provided, monthly granularity is used.

    Returns:
        (DataFrame): a Spark DataFrame with the required metrics for the given period, considering the categories defined in the detail breakdown.

    Examples:
```python
        request_comexstat(
            year_interval={'from': '2024-01', 'to': '2024-03'},
            filters=[
                {
                    'filter': 'heading',
                    'values': heading_codes,
                }
            ],
            metrics=['metricFOB', 'metricKG'],
            details=['country', 'state', 'city', 'heading'],
            flow=flow,
        )
```
    """
    heading_values = [filter['values'] for filter in filters]

    print(f'\n| INFO | Consulting {flow} from {year_interval["from"]} → {year_interval["to"]}')

    payload = {
        'flow': flow,
        'monthDetail': month_detail,
        'period': year_interval,
        'filters': filters,
        'details': details,
        'metrics': metrics,
    }
 
    resp = requests.post(
        f'{BASE_COMEXSTAT_URL}/cities',
        json=payload,
        params={'language': 'pt'},
    )

    if not resp.ok:
        try:
            error_msg = resp.json().get('error', {}).get('message', resp.text)
        except Exception:
            error_msg = resp.text
        raise requests.HTTPError(f'{resp.status_code}: {error_msg}', response=resp)

    else:
        records = resp.json().get('data', {}).get('list', [])

        if not records:
            raise ValueError('API returned empty records list.')

        df_records = spark.createDataFrame(records)
        
        rows = df_records.count()
        print(f'| INFO | Rows read: {rows}')

        return df_records

# Splits the date range into API-safe intervals to avoid silent data truncation:
# 5-year blocks up to 2023, one year per request from 2024 onwards.
def get_year_intervals(date_from: date, date_to: date) -> list[tuple[str, str]]:
    """
    Returns a list of date intervals formatted for the ComexStat API.

    Periods up to 2023 are grouped in 5-year blocks, which is the maximum
    interval the /cities endpoint handles without silently truncating monthly data.
    From 2024 onwards, one request per year is required due to the higher
    trade volume in recent years causing the API to return incomplete months
    when larger intervals are used.

    Args:
        date_from (date): start date of the extraction period.
        date_to (date): end date of the extraction period. Usually the last
            available date from the ComexStat API (/cities/dates/updated).

    Returns:
        list[tuple[str, str]]: list of (from, to) tuples in 'YYYY-MM' format,
            ready to be used as the 'period' parameter in the API payload.

    Examples:
```python
        get_year_intervals(date(2006, 1, 1), date(2026, 3, 1))
        # Returns:
        # [('2006-01', '2010-12'),
        #  ('2011-01', '2015-12'),
        #  ('2016-01', '2020-12'),
        #  ('2021-01', '2023-12'),
        #  ('2024-01', '2024-12'),
        #  ('2025-01', '2025-12'),
        #  ('2026-01', '2026-03')]
```
    """
    intervals = []
    
    # 2006-2023: 5 years chunk
    cutoff_year = 2024
    current = date_from.year
    
    while current < min(cutoff_year, date_to.year + 1):
        end = min(current + 4, cutoff_year - 1, date_to.year)
        from_str = f'{current}-01'
        to_str = f'{end}-12' if end < date_to.year else date_to.strftime('%Y-%m')
        intervals.append((from_str, to_str))
        current = end + 1

    # 2024 forwards: one year by request
    current = max(current, cutoff_year)
    while current <= date_to.year:
        from_str = f'{current}-01'
        to_str = f'{current}-12' if current < date_to.year else date_to.strftime('%Y-%m')
        intervals.append((from_str, to_str))
        current += 1

    return intervals

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Running full/incremental ingestion

# CELL ********************

#=========================================================#
# Reading landing_meta_table
#=========================================================#
df_landing_meta = spark.read.format('delta').load(landing_meta_table_path)

for flow in ['export', 'import']:
    print(f'{"="*50}\n')
    print(f'| INFO | Checking Flow {flow}.\n')

    meta_last_date = df_landing_meta.filter(F.col('flow') == flow) \
        .select(F.max(F.col('date_to')).alias('max_date_to')) \
        .collect()[0]['max_date_to']
    #=========================================================#
    # Getting year intervals for full or incremental ingestion
    #=========================================================#

    # First ingestion — no landing metatable record
    if meta_last_date is None:

        ingestion_type = "Full"

        date_from = date(start_year, 1, 1)

        if final_year is None or final_year == api_last_date.year:
            date_to = api_last_date
            print(f'| INFO | No previous ingestion found. Starting from {date_from} to {date_to}.')
        elif final_year > api_last_date.year:
            date_to = api_last_date
            print(f'| INFO | No data available for {final_year}, running extraction until {api_last_date}.')
        else:
            date_to = date(final_year, 12, 1)
            print(f'| INFO | No previous ingestion found. Starting from {date_from} to {date_to}.')

    # Incremental ingestion
    elif api_last_date > meta_last_date:

        ingestion_type = "Incremental"

        # Skip one month from date_to
        if meta_last_date.month == 12:
            date_from = date(meta_last_date.year + 1, 1, 1)
            date_to = api_last_date
        else:
            date_from = date(meta_last_date.year, meta_last_date.month + 1, 1)
            date_to = api_last_date
        print(f'| INFO | New data available. Ingesting from {date_from} to {date_to}.')

    else:

        ingestion_type = "Skip"

        print(f'| INFO | Flow {flow} is up to date. Skipping...\n')
        continue

    intervals = get_year_intervals(date_from, date_to)

    #=========================================================#
    # Requesting data from year intervals
    #=========================================================#
    target_file = f'{flow}_{date_from.strftime("%Y%m")}_{date_to.strftime("%Y%m")}.parquet'
    target_path = f'{landing_files_path}/{flow}/{target_file}'

    for from_str, to_str in intervals:
        year_interval = {
            'from': from_str,
            'to': to_str,
        }

        df_chunk = request_comexstat(
            year_interval=year_interval,
            filters=[
                {
                    'filter': 'heading',
                    'values': heading_codes,
                }
            ],
            metrics=['metricFOB', 'metricKG'],
            details=['country', 'state', 'city', 'heading'],
            flow=flow,
        )

        df_chunk = df_chunk.withColumn('ingested_at', F.lit(datetime.now()))

        df_chunk.write.mode('append').parquet(target_path)

        print(f'| SUCCESS | Chunk {from_str} → {to_str} extracted and persisted at: {target_path}.')
        sleep(10)

    print(f'\n| SUCCESS | Extraction to "{target_file}" finished.\n')

    #=========================================================#
    # Writing metadata
    #=========================================================#
    meta_record = spark.createDataFrame(
        [(flow, date_from, date_to, target_path, datetime.now())],
        schema=landing_meta_schema
    )

    meta_record.write.format('delta').mode('append').save(landing_meta_table_path)

    print(f'| SUCCESS | Landing metatable updated for flow {flow}.\n')

print(f'{"="*50}\n')
if ingestion_type == "Skip":
    print(f'| INFO | All main files are up to date.')
else:
    print(f'| SUCCESS | Main files extracted successfully')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extracting auxiliary COMEX STAT files

# CELL ********************

for aux in ['uf', 'cities', 'countries']:
    print(f'| INFO | Starting extraction of auxiliary table: {aux}')

    url = f"{BASE_COMEXSTAT_URL}/tables/{aux}"
    print(f'| INFO | Fetching auxiliary table "{aux}" from {url}')

    resp = requests.get(url)

    if not resp.ok:
        try:
            error_msg = resp.json().get('error', {}).get('message', resp.text)
        except Exception:
            error_msg = resp.text
        raise requests.HTTPError(f'{resp.status_code}: {error_msg}', response=resp)

    else:
        data = resp.json().get('data', {})
        records = data.get('list', data) if isinstance(data, dict) else data

        if not records:
            raise ValueError('API returned empty records list.')

        df_records = spark.createDataFrame(records)
        
        rows = df_records.count()
        print(f'| SUCCESS | Table "{aux}" fetched successfully. Rows read: {rows}.')

    df_records = df_records.withColumn('ingested_at', F.lit(datetime.now()))

    target_file = f'aux_{aux}.parquet'
    target_path = f'{landing_files_path}/aux/{target_file}'

    df_records.write.mode('overwrite').parquet(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extracting auxiliary CPI (Consumer Price Index) file

# CELL ********************

flow = 'cpi'
target_file = f'aux_{flow}.parquet'
target_path = f'{landing_files_path}/aux/{target_file}'
series_id = ['CUUR0000SA0']
BLS_API_KEY = notebookutils.credentials.getSecret(key_vault, "bls-api-key")

intervals = []
current_start = start_year
while current_start <= final_year:
    current_end = min(current_start + 10, final_year)
    intervals.append((current_start, current_end))
    current_start = current_end + 1

print(f'| INFO | Starting extraction of "cpi" from {BASE_CPI_URL}. Intervals: {intervals}')

if notebookutils.fs.exists(target_path):
    notebookutils.fs.rm(target_path, recurse=True)

for min_year, max_year in intervals:
    print(f'\n| INFO | Consulting CPI values from {min_year} → {max_year}')

    payload = {
        'seriesid': series_id,
        'startyear': min_year,
        'endyear': max_year,
        'registrationkey': BLS_API_KEY,
    }

    resp = requests.post(BASE_CPI_URL, json=payload)

    if not resp.ok:
        try:
            error_msg = resp.json().get('message', [resp.text])
        except Exception:
            error_msg = resp.text
        raise requests.HTTPError(f'{resp.status_code}: {error_msg}', response=resp)
    else:
        body = resp.json()

        if body.get('status') != 'REQUEST_SUCCEEDED':
            print(f'| WARNING | {body["status"]}: {body["message"]}')

        try:
            series_list = body.get('Results', {}).get('series', [])
            records = series_list[0].get('data', []) if series_list else []

            # Remove 'footnotes' field to avoid Spark type inference errors on empty dicts
            records_clean = [{k: v for k, v in r.items() if k != 'footnotes'} for r in records]
            df_records = spark.createDataFrame(records_clean)

            print(f'| INFO | Rows read: {df_records.count()}')

        except (IndexError, KeyError):
            print(f'| WARNING | No data found for {min_year} → {max_year}. Skipping.')
            continue

    df_records = df_records.withColumn('ingested_at', F.lit(datetime.now()))
    df_records.write.mode('append').parquet(target_path)
    print(f'| SUCCESS | Data from {min_year} to {max_year} persisted at: {target_path}.')

print(f'\n| SUCCESS | CPI extraction completed.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
