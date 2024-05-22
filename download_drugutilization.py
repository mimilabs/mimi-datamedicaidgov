# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # State Drug Utilization Data

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from tqdm import tqdm
path = "/Volumes/mimi_ws_1/datamedicaidgov/src/drugutilization/"

# COMMAND ----------

# Each year, new URLs get added. NOTE: In 2025, we need to manually add a new one...
data_ids = ["d890d3a9-6b00-43fd-8b31-fcba4c8e2909",
       "200c2cba-e58d-4a95-aa60-14b99736808d",
       "eec7fbe6-c4c4-5915-b3d0-be5828ef4e9d",
        "cc318bfb-a9b2-55f3-a924-d47376b32ea3",
        "daba7980-e219-5996-9bec-90358fd156f1",
        "a1f3598e-fc71-51aa-8560-78e7e1a61b09",
        "776a3880-a62d-5990-8b40-4406e6861dbb",
        "53cf9f05-97e3-5bd6-a237-bc971e3642d9"]

# COMMAND ----------

urls = []
for did in data_ids:        
    res = requests.get(("https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/"
                        +did))
    distitems = res.json().get("distribution", [])
    if len(distitems) == 0:
        continue
    url = distitems[0].get("downloadURL")
    if url is not None:
        urls.append(url)


# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

for url in urls:
    filename = url.split('/')[-1]
    download_file(url, filename, path)

# COMMAND ----------


