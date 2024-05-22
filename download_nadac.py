# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # National Average Drug Acquisition Cost (NADAC) 

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from tqdm import tqdm
archive_path = "/Volumes/mimi_ws_1/datamedicaidgov/src/nadac/archives/"
active_path = "/Volumes/mimi_ws_1/datamedicaidgov/src/nadac/active/"

# COMMAND ----------

# Each year, new URLs get added. NOTE: In 2025, we need to manually add a new one...
active_ids = ["99315a95-37ac-4eee-946a-3c523b4c481e"]
archive_ids = [
    "4a00010a-132b-4e4d-a611-543c9521280f",
    "dfa2ab14-06c2-457a-9e36-5cb6d80f8d93",
    "d5eaf378-dcef-5779-83de-acdd8347d68e",
    "c933dc16-7de9-52b6-8971-4b75992673e0",
    "76a1984a-6d69-5e4d-86c8-65eb31f0506d",
    "8de1b213-73c5-552b-b84e-ac795f34d056",
    "1c5d0fc9-693a-534a-8240-4627d9362b0d",
    "7656fc17-f1b4-566b-9a2d-c4a4f2ac7ae1",
    "4d7af295-2132-55a8-b40c-d6630061f3e8",
    "ba0c3734-8012-549a-8f50-2ff389d0e0ef",
    "1fe73992-cbfd-5109-97bc-dee8b33fdcff"
]

# COMMAND ----------

active_urls = []
archive_urls = []
for i, did in enumerate(active_ids + archive_ids):
        
    res = requests.get(("https://data.medicaid.gov/api/1/metastore/schemas/dataset/items/"
                        +did))
    distitems = res.json().get("distribution", [])
    if len(distitems) == 0:
        continue
    url = distitems[0].get("downloadURL")
    if url is not None:
        if i < len(active_ids):
            active_urls.append(url)
        else:
            archive_urls.append(url)

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

for url in archive_urls:
    filename = url.split('/')[-1]
    download_file(url, filename, archive_path)

# COMMAND ----------

for url in active_urls:
    filename = url.split('/')[-1]
    download_file(url, filename, active_path)

# COMMAND ----------


