# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Child and Adult Health Care Quality Measures Quality

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from tqdm import tqdm
path = "/Volumes/mimi_ws_1/datamedicaidgov/src/quality/"

# COMMAND ----------

# Each year, new URLs get added. NOTE: In 2025, we need to manually add a new one...
# 22, 20, ..., 14
data_ids = ["dfd13757-d763-4f7a-9641-3f06ce21b4c6",
            "a058ef78-e18b-4435-94aa-b70ab6ce5904",
            "fbbe1734-b448-4e5a-bc94-3f8688534741",
            "e36d89c0-f62e-56d5-bc7e-b0adf89262b8",
            "229d6279-e614-5353-9226-f6a6f37d06c3",
            "c1028fdf-2e43-5d5e-990b-51ed03428625",
            "fc3c7c14-4b08-59c2-97db-0726e478dfdf",
            "45a28339-17a5-55e6-8e74-e9004fc703d8",
            "2b6a0ec0-efe6-5aec-9fe4-e168b8b6f553"]

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

urls

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


