# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook para realizar a configuração do ambiente

# COMMAND ----------

##### Preencha com o nome do catálogo
catalogo = "workshop_databricks_jsf"

##### Preencha com o nome do prefixo do schema
prefix_db = "ai_"

##### Preencha com o nome do indice do Vector Search
VECTOR_SEARCH_ENDPOINT_NAME = "vs_workshop_23_07"

serving_endpoint_name_prefix = "endpoint_chat_ir_"

##### Preencher com o nome do endpoint de LLM que irá usar
instruct_model= "databricks-meta-llama-3-3-70b-instruct"
embedding_model= 'databricks-bge-large-en'

model_name="ob_chatbot_model"
