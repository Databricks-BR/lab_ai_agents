# Databricks notebook source
# MAGIC %md
# MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
# MAGIC
# MAGIC # Hands-On LAB 02 - Usando Agentes
# MAGIC
# MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Objetivo do exercício
# MAGIC
# MAGIC Este notebook tem como objetivo principal a criação de uma base de conhecimento a partir de documentos PDF. O processo inclui a instalação das bibliotecas necessárias, a extração e estruturação das informações contidas nesses documentos. A seguir, estão as etapas detalhadas:
# MAGIC
# MAGIC 1. **Upload do PDF no Volume**: Faça o upload do arquivo PDF e armazene no volume de armazenamento do Databricks.
# MAGIC 2. **Instalação de Bibliotecas**: Instala as bibliotecas necessárias para manipulação e extração de dados de arquivos PDF.
# MAGIC 3. **Criação da Base de Conhecimento**: Extrai as informações dos PDFs e estrutura os dados em uma base de conhecimento utilizável para análises posteriores.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalação de Bibliotecas

# COMMAND ----------

# MAGIC %pip install mlflow==2.10.2 langchain==0.1.6 databricks-vectorsearch==0.22 databricks-sdk==0.18.0 mlflow[databricks] langchain-community==0.0.19
# MAGIC dbutils.library.restartPython()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuração do ambiente

# COMMAND ----------

# MAGIC %run "./_setup/setup"

# COMMAND ----------

dbutils.widgets.text("volume_path",f"/Volumes/{catalogo}/{schema}/vol_ir","Caminho para o Volume")
dbutils.widgets.text("catalog",catalogo,"Catálogo")
dbutils.widgets.text("schema", schema ,"Schema")
dbutils.widgets.text("vs_endpoint","vs_workshop_23_07","Nome do endpoint de Vector Search")

# COMMAND ----------

volume_path=dbutils.widgets.get("volume_path")
catalog=dbutils.widgets.get("catalog")
sch=dbutils.widgets.get("schema")
vs_endpoint=dbutils.widgets.get("vs_endpoint")

# COMMAND ----------

print(f"Link do volume com o pdf: https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/explore/data/volumes/{catalogo}/{sch}/vol_ir")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar o chat LLM sem a base de conhecimento
# MAGIC
# MAGIC Agora criaremos um chat sem a base de conhecimento, somente com o prompt.

# COMMAND ----------

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_community.chat_models import ChatDatabricks

def chat(message):
    messages = [
        SystemMessage(content="Você é um assistente da receita federal que responde duvidas sobre IRPF no idioma português."),
        HumanMessage(content=message)
        ]

    chat_model = ChatDatabricks(endpoint=instruct_model)
    return (chat_model.invoke(messages).content)

# COMMAND ----------

chat("posso declarar minha sogra no IR?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar o chat com a base de conhecimento (RAG)

# COMMAND ----------

# MAGIC %md
# MAGIC Criando o Vector Search

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
vsc = VectorSearchClient()

if not endpoint_exists(vsc, vs_endpoint):
    vsc.create_endpoint(name=vs_endpoint, endpoint_type="STANDARD")

wait_for_vs_endpoint_to_be_ready(vsc, vs_endpoint)
print(f"Endpoint named {vs_endpoint} is ready.")

# COMMAND ----------


from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

#The table we'd like to index
source_table_fullname = f"{catalog}.{sch}.ir_pdf_doc"
# Where we want to store our index
vs_index_fullname = f"{catalog}.{sch}.{unique_name()}_vs_index"


# COMMAND ----------

vs_index_fullname

# COMMAND ----------


if not index_exists(vsc, vs_endpoint, vs_index_fullname):
  print(f"Creating index {vs_index_fullname} on endpoint {vs_endpoint}...")
  try:
    vsc.create_delta_sync_index(
      endpoint_name=vs_endpoint,
      index_name=vs_index_fullname,
      source_table_name=source_table_fullname,
      pipeline_type="TRIGGERED", #Sync needs to be manually triggered
      primary_key="id",
      embedding_source_column="chunk",
      embedding_model_endpoint_name= embedding_model
    )
  except Exception as e:
    display_quota_error(e, vs_endpoint)
    raise e
  #Let's wait for the index to be ready and all our embeddings to be created and indexed
  wait_for_index_to_be_ready(vsc, vs_endpoint, vs_index_fullname)
else:
  #Trigger a sync to update our vs content with the new data saved in the table
  wait_for_index_to_be_ready(vsc, vs_endpoint, vs_index_fullname)
  vsc.get_index(vs_endpoint, vs_index_fullname).sync()

# COMMAND ----------

print(f"Link do Vector Search criado: https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}/explore/data/{catalogo}/{sch}/{unique_name()}_vs_index")

# COMMAND ----------

# MAGIC %md
# MAGIC Testando o vector search criado:

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from langchain_community.vectorstores import DatabricksVectorSearch
from langchain_community.embeddings import DatabricksEmbeddings

embedding = DatabricksEmbeddings(endpoint=embedding_model)

def get_retriever(persist_dir: str = None):
    #Get the vector search index
    vsc = VectorSearchClient(disable_notice=True)
    vs_index = vsc.get_index(
        endpoint_name=vs_endpoint,
        index_name=vs_index_fullname
    )

    # Create the retriever
    vectorstore = DatabricksVectorSearch( 
        vs_index, text_column="chunk", embedding=embedding
    )
    return vectorstore.as_retriever(search_kwargs={'k': 3})

# test our retriever
vectorstore = get_retriever()
similar_documents = vectorstore.get_relevant_documents("posso cancelar o compartilhamento?")

print(f"Relevant documents: {similar_documents[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando o Agente RAG
# MAGIC
# MAGIC Criando o chat adicionando um prompt e o conteúdo encontrado no vector search para complementar a resposta do LLM.

# COMMAND ----------

from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_community.chat_models import ChatDatabricks

TEMPLATE = """Você é um assistente da receita federal que responde dúvidas sobre o imposto de renda. Todas as respostas devem ser baseadas no contexto abaixo.
Caso o contexto abaixo não fornece uma resposta precisa, responda "Desculpe, não tenho informações suficientes para responder esta pergunta."
Assuntos não relacionados ao imposto de renda não devem ser respondidos.
{context}
Pergunta: {question}
Resposta:
"""
prompt = PromptTemplate(template=TEMPLATE, input_variables=["context", "question"])
chat_model = ChatDatabricks(endpoint=instruct_model)

chain = RetrievalQA.from_chain_type(
    llm=chat_model,
    chain_type="stuff",
    retriever=get_retriever(),
    chain_type_kwargs={"prompt": prompt}
)

# COMMAND ----------

# MAGIC %md
# MAGIC Testando o RAG

# COMMAND ----------

import langchain

langchain.debug = False
question = {"query": "Posso atualizar o valor do imovel?", "columns" : ["response"]}
answer = chain.invoke(question)
print(answer['result'])

# COMMAND ----------


