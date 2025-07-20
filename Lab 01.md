<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>

# Hands-On Labs - Criando Agentes de IA

Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.
</br></br>

# Lab 01 - Step by step


## 1. Faça o download do .zip para sua máquina

1. [Baixe o arquivo .zip](https://github.com/Databricks-BR/lab_ai_agents/blob/main/notebooks.zip)
2. Importe o arquivo .zip no Databricks

---

## 2. Acessar o notebook 00. Setup

- Clique em *edit* > *add parameter*
- Crie um parâmetro chamado **"iniciais"**
- Na caixa de texto que apareceu, digite as iniciais do seu nome
- Execute a célula com o SQL
- Faça o download dos arquivos `.csv` que estão no diretório:
  - `notebooks/01. Busca Médico/data`
- Faça o upload dos arquivos para o volume `vol_agent`

---

## 3. Acessar o notebook 01. Preparando os dados

- Clique em *edit* > *add parameter*
- Crie um parâmetro chamado **"iniciais"**
- Substitua na linha 12 e 23 o `<NOME_DO_SEU_CATALOGO>` pelo nome do seu catálogo
- Execute o notebook 01
- Confirme se o resultado informa que há **1.000** registros em cada tabela

---

## 4. Acessar o notebook 02. Create Functions

- Execute a primeira célula para criar os **parâmetros**
  - Inclua suas iniciais no parâmetro **"iniciais"**
  - Execute o comando *Run all*
  - Verifique se todas as linhas obtiveram sucesso na execução

---

## 5. Acessar o notebook 03. Prompt

1. Copie todo o texto da célula
2. Clique em Playground
3. Selecione o modelo **Meta Llama 3.3 70B Instruct**
4. Clique em *Add system prompt* e cole o conteúdo do notebook 03. Prompt
5. Clique em **Tools > Add Tools**
6. Selecione:  
   `workshop_databricks_suasIniciais.agents.*`
7. Pergunte:  
   `"Qual o cardiologista mais perto de mim?"`
8. Clique em **get Code e selecciona Agent Notebook**
9. Altere o nome do notebook do driver para **AgentBuscaMedico**
10. Altere o nome da pasta para **AgentBuscaMedico**
11. Na célula 16, coloque seu catálogo e schema.  
    Em Model Name coloque:  
    `AgentBuscaMedico_suas_iniciais`
12. Execute as células
13. Explore seu **Agent**

