<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>

# Hands-On Labs - Criando Agentes de IA

Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.
</br></br>

# Lab 01 - Step by step


## 1. Crie uma git folder no Databricks

1.1. Clique em workspace no canto superior esquerdo
1.2. No canto superior direito, clique em create e selecione "git folder""
1.3. Em Git Repository URL, digite -> https://github.com/Databricks-BR/lab_ai_agents/

---

## 2. Acessar o notebook /notebooks/01. Busca Médico/00. Setup

2.1. Na caixa de texto chamada **iniciais**, digite as iniciais do seu nome
2.2. Execute a célula com o SQL

## 3. Faça o download dos arquivos `.csv` que estão no diretório: 
  3.1. Faça o download dos arquivos `.csv` que estão no diretório: `notebooks/01. Busca Médico/data`
  3.2. Faça o upload dos arquivos para o volume `vol_agent`

---

## 4. Acessar o notebook /notebooks/01. Busca Médico/01. Preparando os dados

4.1. Na caixa de texto chamada **iniciais**, digite as iniciais do seu nome
4.2. Substitua na linha 12 e 23 o `<NOME_DO_SEU_CATALOGO>` pelo nome do seu catálogo
4.3. Execute o notebook 01
4.4. Confirme se o resultado informa que há **1.000** registros em cada tabela

---

## 5. Acessar o notebook /notebooks/01. Busca Médico/02. Create Functions

 5.1. Inclua suas iniciais no parâmetro **"iniciais"**
 5.2. Execute o comando *Run all*
 5.3. Verifique se todas as células obtiveram sucesso na execução

---

## 6. Acessar o notebook /notebooks/01. Busca Médico/03. Prompt

6.1. Copie todo o texto da célula
6.2. Clique em Playground
6.3. Selecione o modelo **Meta Llama 3.1 405B Instruct**
6.4. Clique em *Add system prompt* e cole o conteúdo do notebook 03. Prompt
6.5. Clique em **Tools > Add Tools**
6.6. Selecione:  
   `workshop_databricks_suasIniciais.agents.*`
6.7. Pergunte:  
   `"Qual o cardiologista mais perto de mim?"`
6.8. Clique em **get Code e selecciona Agent Notebook**
6.9. Altere o nome do notebook do driver para **AgentBuscaMedico**
6.10. Altere o nome da pasta para **AgentBuscaMedico**
6.11. Na célula 16, coloque seu catálogo e schema.  
    Em Model Name coloque:  
    `AgentBuscaMedico_suas_iniciais`
6.12. Execute as células
6.13. Explore seu **Agent**

