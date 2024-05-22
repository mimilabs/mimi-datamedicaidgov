# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the NADAC files
# MAGIC

# COMMAND ----------

from pathlib import Path
import csv
from pyspark.sql.functions import col, lit, to_date, substring
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import re

refresh_archives = True
path = "/Volumes/mimi_ws_1/datamedicaidgov/src" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "datamedicaidgov" # delta table destination schema
tablename = "nadac" # destination table

# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

files = []
if refresh_archives:
    datemap = {
        "nadac-national-average-drug-acquisition-cost-12-27-2023": "2023-12-31",
        "nadac-national-average-drug-acquisition-cost-2022": "2022-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.1c5d0fc9-693a-534a-8240-4627d9362b0d": "2017-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.1fe73992-cbfd-5109-97bc-dee8b33fdcff": "2013-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.4d7af295-2132-55a8-b40c-d6630061f3e8": "2015-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.7656fc17-f1b4-566b-9a2d-c4a4f2ac7ae1": "2016-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.76a1984a-6d69-5e4d-86c8-65eb31f0506d": "2019-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.8de1b213-73c5-552b-b84e-ac795f34d056": "2018-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.ba0c3734-8012-549a-8f50-2ff389d0e0ef": "2014-12-31",
        "nadac-national-average-drug-acquisition-cost.a4y5-998d.c933dc16-7de9-52b6-8971-4b75992673e0": "2020-12-31",
        "national-average-drug-acquisition-cost-12-29-2021": "2021-12-31"
    }
    for filepath in Path(f"{path}/{tablename}/archives").glob("*.csv"):
        datestr = datemap[filepath.stem]
        dt = parse(datestr).date()
        files.append((dt, filepath, "archive"))
for filepath in Path(f"{path}/{tablename}/active").glob("*.csv"):
    datestr = filepath.stem[-8:]
    dt = datetime.strptime(datestr, "%m%d%Y").date()
    files.append((dt, filepath, "active"))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

assert(len([x for x in files if x[2]=="active"]) < 2)

# COMMAND ----------

for fileindex, item in enumerate(files):
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        if col_new in {"nadac_per_unit", "corresponding_generic_drug_nadac_per_unit"}:
            df = df.withColumn(col_new, col(col_old).cast("double"))
        elif col_new in {"effective_date", "corresponding_generic_drug_effective_date", "as_of_date"}:
            df = df.withColumn(col_new, to_date(substring(col(col_old), 1, 10), "M/d/y"))
        else:        
            df = df.withColumn(col_new, col(col_old))
        
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    if refresh_archives:
        writemode = "append"
        if fileindex == 0:
            writemode = "overwrite"
        (df.write
            .format('delta')
            .mode(writemode)
            .option("mergeSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    else:
        # Assumes there is only "one" active file
        (df.write
            .format('delta')
            .mode("overwrite")
            .option("replaceWhere", "active_or_archive = 'active'")
            .option("mergeSchema", "true")
            .saveAsTable(f"{catalog}.{schema}.{tablename}"))
        

# COMMAND ----------


