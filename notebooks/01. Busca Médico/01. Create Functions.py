# Databricks notebook source
# MAGIC %md
# MAGIC %md <img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>
# MAGIC
# MAGIC # Hands-On LAB 01 - Criando funções
# MAGIC
# MAGIC Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.

# COMMAND ----------

# MAGIC %md
# MAGIC # Define as Variáveis

# COMMAND ----------

dbutils.widgets.text("iniciais", "")
iniciais = dbutils.widgets.get("iniciais")

# COMMAND ----------

dbutils.widgets.text("CATALOG", f"workshop_databricks_{iniciais}")
dbutils.widgets.text("SCHEMA", "agents")
dbutils.widgets.text("medicos_table", f"{dbutils.widgets.get('CATALOG')}.{dbutils.widgets.get('SCHEMA')}.medicos")
dbutils.widgets.text("pacientes_table", f"{dbutils.widgets.get('CATALOG')}.{dbutils.widgets.get('SCHEMA')}.pacientes")

# COMMAND ----------

CATALOG = dbutils.widgets.get("CATALOG")
SCHEMA = dbutils.widgets.get("SCHEMA")
medicos_table = dbutils.widgets.get('medicos_table')
pacientes_table = dbutils.widgets.get('pacientes_table')

# COMMAND ----------

# MAGIC %md
# MAGIC # Cria Funções

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca Paciente

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.buscar_paciente_por_cpf(
    cpf_paciente STRING COMMENT 'CPF do paciente que será usado para buscar os dados'
  )
  RETURNS TABLE(
    nome STRING COMMENT 'Nome completo do paciente',
    cpf STRING COMMENT 'CPF do paciente',
    endereco STRING COMMENT 'Endereço completo do paciente',
    plano STRING COMMENT 'Plano de saúde do paciente'
  )
  COMMENT 'Função para buscar os dados de um paciente com base no CPF fornecido. A função retorna as informações do paciente da tabela pacientes.'
  RETURN
    SELECT
      nome,
      cpf,
      endereco,
      plano
    FROM
      {pacientes_table}
    WHERE
      cpf = cpf_paciente;""")

# COMMAND ----------

spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.buscar_paciente_por_cpf('51749280620')").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Busca Médicos

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.encontrar_medicos_proximos(
  lat_usuario DOUBLE COMMENT 'Latitude do usuário',
  long_usuario DOUBLE COMMENT 'Longitude do usuário',
  especialidade_usuario STRING COMMENT 'Especialidade médica desejada pelo usuário',
  plano_usuario STRING COMMENT 'Plano de saúde do usuário',
  raio_km INT DEFAULT 20 COMMENT 'Raio de busca em quilômetros'
)
RETURNS TABLE(
  nome_medico STRING COMMENT 'Nome do médico',
  especialidade STRING COMMENT 'Especialidade do médico',
  endereco STRING COMMENT 'Endereço do consultório',
  bairro STRING COMMENT 'Bairro do consultório',
  planos_aceitos ARRAY<STRING> COMMENT 'Lista de planos aceitos pelo médico (como array)',
  latitude DOUBLE COMMENT 'Latitude do consultório',
  longitude DOUBLE COMMENT 'Longitude do consultório',
  distancia_km DOUBLE COMMENT 'Distância em quilômetros entre o usuário e o médico'
)
COMMENT 'Função v3: Usa array_contains para planos e calcula distância após filtros.'
RETURN
  WITH medicos_filtrados AS (
    -- Passo 1: Aplicar filtros simples primeiro
    SELECT
      m.nome_medico,
      m.especialidade,
      m.endereco,
      m.bairro,
      m.planos_aceitos,
      m.latitude,
      m.longitude
    FROM
      {medicos_table} m
    WHERE
      -- Filtro de especialidade
      (especialidade_usuario IS NULL OR UPPER(m.especialidade) = UPPER(especialidade_usuario))
      -- Filtro de plano OTIMIZADO usando array_contains
      AND array_contains(m.planos_aceitos, plano_usuario)
      -- Filtro espacial inicial (bounding box)
      AND m.latitude BETWEEN lat_usuario - (raio_km / 111.0) AND lat_usuario + (raio_km / 111.0)
      AND m.longitude BETWEEN long_usuario - (raio_km / (111.0 * cos(radians(lat_usuario)))) AND long_usuario + (raio_km / (111.0 * cos(radians(lat_usuario))))
  ),
  resultados_com_distancia AS (
    -- Passo 2: Calcular a distância APENAS para os médicos filtrados
    SELECT
      mf.nome_medico,
      mf.especialidade,
      mf.endereco,
      mf.bairro,
      mf.planos_aceitos,
      mf.latitude,
      mf.longitude,
      (6371 * acos(
        cos(radians(lat_usuario)) *
        cos(radians(mf.latitude)) *
        cos(radians(mf.longitude) - radians(long_usuario)) +
        sin(radians(lat_usuario)) *
        sin(radians(mf.latitude))
      )) AS distancia_km
    FROM
      medicos_filtrados mf
  )
  -- Passo 3: Aplicar o filtro final de raio, ordenar e limitar
  SELECT
    r.nome_medico,
    r.especialidade,
    r.endereco,
    r.bairro,
    r.planos_aceitos,
    r.latitude,
    r.longitude,
    r.distancia_km
  FROM resultados_com_distancia r
  WHERE
    r.distancia_km <= raio_km
  ORDER BY
    distancia_km ASC
  LIMIT 10;""")

# COMMAND ----------

spark.sql(
    f"""SELECT * FROM {CATALOG}.{SCHEMA}.encontrar_medicos_proximos
                (
                  -23.533773, 
                  -46.625290, 
                  'Cardiologia', 
                  'Diamante',
                  20
                )"""
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calcular Lat & Long

# COMMAND ----------

spark.sql(
    f"""CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.geocode_endereco(endereco STRING)
        RETURNS STRUCT<latitude DOUBLE, longitude DOUBLE>
        LANGUAGE PYTHON
        AS
        $$
        import http.client
        import json
        import urllib.parse
        import time

        def geocode_via_nominatim(address):
            # Codificar o endereço para URL
            encoded_address = urllib.parse.quote(address)
            
            # Configurar a conexão HTTPS
            conn = http.client.HTTPSConnection('nominatim.openstreetmap.org')
            
            # Adicionar um User-Agent válido (obrigatório para Nominatim)
            headers = {{ 
                'User-Agent': 'Databricks-Geocoding-Function/1.0',
                'Accept': 'application/json'
            }} 
            
            # Construir o caminho da requisição
            request_path = f'/search?q={{encoded_address}}&format=json&limit=1'
            
            # Fazer a requisição GET
            conn.request('GET', request_path, headers=headers)
            
            # Obter a resposta
            response = conn.getresponse()
            
            # Ler e decodificar a resposta JSON
            data = json.loads(response.read().decode())
            
            # Verificar se a resposta contém resultados
            if data and len(data) > 0:
                # Extrair latitude e longitude
                return (float(data[0]['lat']), float(data[0]['lon']))
            else:
                # Retornar None em caso de erro
                return (None, None)

        result = geocode_via_nominatim(endereco)
        return {{'latitude': result[0], 'longitude': result[1]}}
        $$;"""
)

# COMMAND ----------

spark.sql(f"SELECT {CATALOG}.{SCHEMA}.geocode_endereco('Avenida Paulista, 1000 - Bela Vista, São Paulo - SP')").display()
