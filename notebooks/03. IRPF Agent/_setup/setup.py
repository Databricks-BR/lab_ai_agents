# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch==0.22
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./env"

# COMMAND ----------

catalogo

# COMMAND ----------

def clean_string(value, replacement: str = "_") -> str:
    import re
    replacement_2x = replacement+replacement
    value = re.sub(r"[^a-zA-Z\d]", replacement, str(value))
    while replacement_2x in value:
        value = value.replace(replacement_2x, replacement)
    return value

# COMMAND ----------

def get_username():
  row = spark.sql("SELECT current_user() as username, current_catalog() as catalog, current_database() as schema").first()
  return row["username"]

# COMMAND ----------

def get_workspace_id():
  return dbutils.entry_point.getDbutils().notebook().getContext().workspaceId().getOrElse(None)

# COMMAND ----------

def unique_name():
  local_part = get_username().split("@")[0]
  hash_basis = f"{get_username()}{get_workspace_id()}"
  # username_hash = stable_hash(hash_basis, length=4)
  # name = f"{local_part} {username_hash}"

  name = f"{local_part}_my"
  return clean_string(name).lower()

# COMMAND ----------

schema =  unique_name()
spark.sql(f"create database if not exists {catalogo}.{schema}")

# COMMAND ----------

serving_endpoint_name = serving_endpoint_name_prefix + unique_name()

# COMMAND ----------

clear = False

if clear:
  spark.sql(f"use catalog {catalogo}")
  # Get all tables and views in the schema
  tables = spark.sql(f"SHOW TABLES IN {schema}").filter("isTemporary == false").select("tableName").collect()

  # Drop all tables and views
  for table in tables:
      table_name = table["tableName"]
      spark.sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")

  # Get all functions in the schema
  functions = spark.sql(f"SHOW FUNCTIONS IN {schema}").select("function").collect()

  # Drop all functions
  for function in functions:
      function_name = function["function"]
      spark.sql(f"DROP FUNCTION IF EXISTS {schema}.{function_name}")

  # Drop all models
  for model in models:
      model_name = model["function"]
      spark.sql(f"DROP MODEL IF EXISTS {schema}.{function_name}")

# COMMAND ----------

spark.sql(f"use {catalogo}.{schema}")
volume_folder = f"/Volumes/{catalogo}/{schema}/landing"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema}.landing")

contratos_folder = f"{volume_folder}/contratos"

# Crie a nova pasta dentro do volume
dbutils.fs.mkdirs(contratos_folder)

import requests 

def download_file(url, destination):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        print('saving ' + destination + '/' + local_filename)
        with open(destination + '/' + local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename
  


# COMMAND ----------

import time
def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

def index_exists(vsc, endpoint_name, index_full_name):
    try:
        dict_vsindex = vsc.get_index(endpoint_name, index_full_name).describe()
        return dict_vsindex.get('status').get('ready', False)
    except Exception as e:
        if 'RESOURCE_DOES_NOT_EXIST' not in str(e):
            print(f'Unexpected error describing the index. This could be a permission issue.')
            raise e
    return False
    
def wait_for_index_to_be_ready(vsc, vs_endpoint_name, index_name):
  for i in range(180):
    idx = vsc.get_index(vs_endpoint_name, index_name).describe()
    index_status = idx.get('status', idx.get('index_status', {}))
    status = index_status.get('detailed_state', index_status.get('status', 'UNKNOWN')).upper()
    url = index_status.get('index_url', index_status.get('url', 'UNKNOWN'))
    if "ONLINE" in status:
      return
    if "UNKNOWN" in status:
      print(f"Can't get the status - will assume index is ready {idx} - url: {url}")
      return
    elif "PROVISIONING" in status:
      if i % 40 == 0: print(f"Waiting for index to be ready, this can take a few min... {index_status} - pipeline url:{url}")
      time.sleep(10)
    else:
        raise Exception(f'''Error with the index - this shouldn't happen. DLT pipeline might have been killed.\n Please delete it and re-run the previous cell: vsc.delete_index("{index_name}, {vs_endpoint_name}") \nIndex details: {idx}''')
  raise Exception(f"Timeout, your index isn't ready yet: {vsc.get_index(index_name, vs_endpoint_name)}")

# COMMAND ----------

import time

def endpoint_exists(vsc, vs_endpoint_name):
  try:
    return vs_endpoint_name in [e['name'] for e in vsc.list_endpoints().get('endpoints', [])]
  except Exception as e:
    #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
    if "REQUEST_LIMIT_EXCEEDED" in str(e):
      print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. The demo will consider it exists")
      return True
    else:
      raise e

def wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint_name):
  for i in range(180):
    try:
      endpoint = vsc.get_endpoint(vs_endpoint_name)
    except Exception as e:
      #Temp fix for potential REQUEST_LIMIT_EXCEEDED issue
      if "REQUEST_LIMIT_EXCEEDED" in str(e):
        print("WARN: couldn't get endpoint status due to REQUEST_LIMIT_EXCEEDED error. Please manually check your endpoint status")
        return
      else:
        raise e
    status = endpoint.get("endpoint_status", endpoint.get("status"))["state"].upper()
    if "ONLINE" in status:
      return endpoint
    elif "PROVISIONING" in status or i <6:
      if i % 20 == 0: 
        print(f"Waiting for endpoint to be ready, this can take a few min... {endpoint}")
      time.sleep(10)
    else:
      raise Exception(f'''Error with the endpoint {vs_endpoint_name}. - this shouldn't happen: {endpoint}.\n Please delete it and re-run the previous cell: vsc.delete_endpoint("{vs_endpoint_name}")''')
  raise Exception(f"Timeout, your endpoint isn't ready yet: {vsc.get_endpoint(vs_endpoint_name)}")

# COMMAND ----------

#Display a better quota message 
def display_quota_error(e, ep_name):
  if "QUOTA_EXCEEDED" in str(e): 
    displayHTML(f'<div style="background-color: #ffd5b8; border-radius: 15px; padding: 20px;"><h1>Error: Vector search Quota exceeded in endpoint {ep_name}</h1><p>Please select another endpoint in the ../config file (VECTOR_SEARCH_ENDPOINT_NAME="<your-endpoint-name>"), or <a href="/compute/vector-search" target="_blank">open the vector search compute page</a> to cleanup resources.</p></div>')

# COMMAND ----------

print(f"Catálogo que você está usando: {catalogo}")
print(f"Schema criado para você: {schema}")

dbutils.widgets.text("catalogo", catalogo)
dbutils.widgets.text("schema", schema)

print(f"Link do seu schema: https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/explore/data/{catalogo}/{schema}")

spark.sql(f"USE CATALOG {catalogo}")
spark.sql(f"USE SCHEMA {schema}")
