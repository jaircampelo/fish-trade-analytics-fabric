# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Brazilian Fish Trade Balance Analysis — Silver to Gold
# 
# ㅤ
# 
# > ㅤ\
# > This project uses the **ComexStat API** from Brazil's Ministry of Development, Industry, Trade and Services (MDIC), which provides data on Brazilian foreign trade.\
# > ㅤ
# 
# ㅤ
# 
# **Notebook:** nb_dag.ipynb
# 
# **Description:** This PySpark notebook just orchestrate all the notebooks in logical sequence due their dependencies. 

# CELL ********************

DAG = {
    'activities': [
        {
            'name': 'extract_to_landing',
            'path': 'nb_extract_to_landing',
            'timeoutPerCellInSeconds': 1200,
        },
        {
            'name': 'landing_to_bronze',
            'path': 'nb_landing_to_bronze',
            'timeoutPerCellInSeconds': 1200,
            'dependencies': ['extract_to_landing'],
        },
        {
            'name': 'bronze_to_silver',
            'path': 'nb_bronze_to_silver',
            'timeoutPerCellInSeconds': 1200,
            'dependencies': ['landing_to_bronze'],
        },
        {
            'name': 'silver_to_gold',
            'path': 'nb_silver_to_gold',
            'timeoutPerCellInSeconds': 1200,
            'dependencies': ['bronze_to_silver'],
        },
    ],
    "timeoutInSeconds": 43200,
}

results = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
