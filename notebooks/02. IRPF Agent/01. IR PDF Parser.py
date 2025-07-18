# Databricks notebook source
# MAGIC %md
# MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
# MAGIC
# MAGIC # Hands-On LAB 02 - Preparando uma base de conhecimento
# MAGIC
# MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.

# COMMAND ----------

dbutils.widgets.text("CATALOG", "databricks_workshop_jsf")
dbutils.widgets.text("SCHEMA", "agents")

# COMMAND ----------

# MAGIC %pip install PyMuPDF
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")

# COMMAND ----------

import re

# COMMAND ----------

import fitz

def read_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text.replace("\n", "")

# COMMAND ----------

# DBTITLE 1,Path to Original PDF
pdf_path = "/Volumes/workshop_databricks_jsf/agents/vol_ir/P&R IRPF 2024 - v1.0 - 2024.05.03.pdf"

# COMMAND ----------

pdf_text = read_pdf(pdf_path)
regex_pattern = r"\d{3} [—-](.*?)Retorno ao sumário"

# COMMAND ----------

lista_perguntas = re.findall(regex_pattern,pdf_text)

# COMMAND ----------

lista_perguntas_respostas = [x.strip().split("?", 1) for x in lista_perguntas]
lista_perguntas_respostas_qm = [
    [
        {
          "Pergunta": p + "?",
          "Resposta": r.strip()
        }
    ]
    for p, r in lista_perguntas_respostas
]

lista_perguntas_respostas_qm = [[entry] for entry in lista_perguntas_respostas_qm]

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, to_json, col

schema = StructType([
    StructField('chunk', ArrayType(
        StructType([
            StructField('Pergunta', StringType(), True),
            StructField('Resposta', StringType(), True)
        ]), True), True)
])

df = spark.createDataFrame(lista_perguntas_respostas_qm, schema)
df = df.withColumn('chunk', to_json(col('chunk')))
df = df.withColumn('id', monotonically_increasing_id())

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ir_pdf_doc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workshop_databricks_jsf.agents.ir_pdf_doc

# COMMAND ----------

# MAGIC %md
# MAGIC Ativando o log da tabela para caso quisermos que a cada nova informação na tabela, a nossa base de conhecimento atualize.

# COMMAND ----------

spark.sql(f"alter table {CATALOG}.{SCHEMA}.ir_pdf_doc set TBLPROPERTIES  (delta.enableChangeDataFeed = true)")
