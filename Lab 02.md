<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width=100%>

# Hands-On Labs - Criando Agentes de IA

Treinamento Hands-on na plataforma Databricks com foco nas funcionalidades de IA Generativa.
</br></br>

# Lab 02: Step by step

## 1. Crie o volume e faça o upload do PDF

1. No schema `Agents` do seu catálogo, crie um volume chamado `vol_ir`
2. Faça o download do PDF que está em `02. Agent IRPF -> Data`
3. Faça o upload do PDF para o volume `vol_ir`

---

## 2. Ajuste ambientes e notebooks

4. Ajuste o notebook `env` que está em `02. Agent IRPF -> _setup`

---

## 3. Execute o IR Parser

5. Abra o notebook **01. IR Parser**
   - Execute a célula 3
   - Ajuste os parâmetros necessários
   - Clique em *Run All*
   - Avalie os resultados

---

## 4. Execute o RAG Chatbot

6. Abra o notebook **02. Rag Chatbot IR - VS**
   - Execute as células 4, 6, 7 e 8
   - Ajuste os parâmetros `catalog` e `schema`

---

## 5. Playground e ajustes finais

7. Vá para o Playground e use **Sonnet 3.7** ou **Llama 3.3 70B**

   - Adicione seu Vector Search Index como tool
   - Pergunte: `"Posso declarar a minha sogra no IR?"`
   - Altere o nome do notebook de driver para `AgentIR`
   - Altere o nome da pasta para `AgentIR`
   - Na célula 16, coloque seu catálogo e schema.  
     Em `Model Name`, insira:  
     ```
     AgentIR_suas_iniciais
     ```

