# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the State Drug Util files
# MAGIC

# COMMAND ----------

from pathlib import Path
import csv
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import re

refresh_archives = True
path = "/Volumes/mimi_ws_1/datamedicaidgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datamedicaidgov" # delta table destination schema
tablename = "quality" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for filepath in Path(f"{path}/{tablename}").glob("*.csv"):
    year = filepath.stem[:4]
    dt = parse(f"{year}-12-31").date()
    if dt not in files_exist:
        files.append((dt, filepath, "active"))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

legacymap = {"25th_percentile": "bottom_quartile",
             "75th_percentile": "top_quartile",
             "measure_description": "rate_definition",
             "state_specific_comments": "statespecific_comments"}
skips = {"location"}

for fileindex, item in enumerate(files):
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):

        col_new = legacymap.get(col_new_, col_new_)
        if col_new in skips:
            continue

        header.append(col_new)
        if col_new in {"ffy", "number_of_states_reporting"}:
            df = df.withColumn(col_new, col(col_old).cast("int"))
        elif col_new in {"state_rate", 
                         "median", 
                         "bottom_quartile", 
                         "top_quartile"}:
            df = df.withColumn(col_new, regexp_replace(col(col_old), r'[^0-9\.]', "").cast("double"))
        else:        
            df = df.withColumn(col_new, col(col_old))
        
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    (df.write
        .format('delta')
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))

# COMMAND ----------


