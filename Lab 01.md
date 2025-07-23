<img src="https://github.com/Databricks-BR/lab_genai/blob/main/img/header.png?raw=true" width="100%">

# 🧠 Hands-On Labs - Criando Agentes de IA

Treinamento *hands-on* na plataforma **Databricks** com foco nas funcionalidades de **IA Generativa**.  
<br><br>

---

## 🔧 Lab 01 - Step by Step

### 1. Crie uma Git Folder no Databricks

1.1. Clique em **Workspace** no canto superior esquerdo <br>
1.2. No canto superior direito, clique em **Create** e selecione **Git Folder** <br>
1.3. Em *Git Repository URL*, insira:  
`https://github.com/Databricks-BR/lab_ai_agents/`  
<br>

---

### 2. Acesse o notebook `/notebooks/01. Busca Médico/00. Setup`

2.1. Na caixa de texto chamada **iniciais**, digite as iniciais do seu nome <br>
2.2. Execute a célula com o código **SQL**  
<br>

---

### 3. Faça o upload dos arquivos `.csv`

3.1. Baixe os arquivos `.csv` no diretório:  
`notebooks/01. Busca Médico/data` <br>
3.2. Faça o upload dos arquivos para o volume **vol_agent**  
<br>

---

### 4. Acesse o notebook `/notebooks/01. Busca Médico/01. Preparando os dados`

4.1. Na caixa de texto chamada **iniciais**, digite as iniciais do seu nome <br>
4.2. Substitua nas linhas **12** e **23** o valor `<NOME_DO_SEU_CATALOGO>` pelo nome do seu catálogo <br>
4.3. Execute todo o notebook <br>
4.4. Verifique se o resultado informa que há **1.000 registros** em cada tabela  
<br>

---

### 5. Acesse o notebook `/notebooks/01. Busca Médico/02. Create Functions`

5.1. Preencha o parâmetro **"iniciais"** com suas iniciais <br>
5.2. Execute o comando **Run all** <br>
5.3. Confirme se todas as células foram executadas com sucesso  
<br>

---

### 6. Acesse o notebook `/notebooks/01. Busca Médico/03. Prompt`

6.1. Copie todo o conteúdo da célula <br>
6.2. Acesse o **Playground** <br>
6.3. Selecione o modelo: **Meta Llama 3.1 405B Instruct** <br>
6.4. Clique em **Add system prompt** e cole o conteúdo do notebook <br>
6.5. Vá em **Tools > Add Tools** <br>
6.6. Selecione:  
`workshop_databricks_suasIniciais.agents.*` <br>
6.7. Faça a pergunta: `"Qual o dermatologista mais perto de mim?"` <br>
6.8. Clique em **Get Code** e selecione **Agent Notebook** <br>
6.9. Altere o nome do notebook para **AgentBuscaMedico** <br>
6.10. Altere o nome da pasta para **AgentBuscaMedico** <br>
6.11. Na célula **16**, preencha seu **catálogo** e **schema**.  
Em *Model Name*, use: `AgentBuscaMedico_suas_iniciais` <br>
6.12. Execute todas as células <br>
6.13. Explore seu **Agente de IA!**  
<br>

---
