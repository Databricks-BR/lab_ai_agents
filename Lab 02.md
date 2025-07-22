<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>

# Hands-On Labs - Criando Agentes de IA

Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.
</br></br>

# Lab 02: Step by step

## 1. Crie o volume e faça o upload do PDF

   1.1. No schema `Agents` do seu catálogo, crie um volume chamado `vol_ir`
   1.2. Faça o download do PDF que está em `02. Agent IRPF -> Data`
   1.3. Faça o upload do PDF para o volume `vol_ir`

---

## 2. Ajuste ambientes e notebooks

   2.1. Ajuste a variável **catalogo** no notebook `env` que está em `02. Agent IRPF -> _setup`


---

## 3. Execute o IR Parser

   3.1. Abra o notebook **01. IR Parser**
   3.2. Ajuste os parâmetros necessários
   3.3. Clique em *Run All*
   3.4. Avalie os resultados

---

## 4. Execute o RAG Chatbot

   4.1. Abra o notebook **02. Rag Chatbot IR - VS**
   4.2 Ajuste os parâmetros `catalog` e `schema`
   4.3 Execute 

---

## 5. Playground e ajustes finais

   5.1. Vá para o Playground e use **Sonnet 3.7** ou **Llama 3.3 70B**

   5.2. Adicione seu Vector Search Index como tool
   5.3. Adicione o seguinte prompt `"Você é um assistente da receita que tira dúvidas sobre o imposto de renda (IR). O VS: workshop_databricks_jsf.agents.figueiro_juliandro_my_vs_index, é retornar a FAQ do IR "`
   5.4. Pergunte: `"Posso declarar a minha sogra no IR?"`
   5.5. Altere o nome do notebook de driver para `AgentIR`
   5.6. Altere o nome da pasta para `AgentIR`
   5.7. Na célula 16, coloque seu catálogo e schema.  
   5.8. Em `Model Name`, insira:  ```AgentIR_suas_iniciais```


