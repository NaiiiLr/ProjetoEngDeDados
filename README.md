#  Data Platform: E-commerce Architecture

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-F68A1E?style=for-the-badge&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![MkDocs](https://img.shields.io/badge/MkDocs-Material-526EE5?style=for-the-badge)

Este repositório contém a implementação prática de uma infraestrutura de dados simulando o ecossistema de um E-commerce. O objetivo principal do projeto é realizar um estudo comparativo entre os dois principais *Open Table Formats* do mercado: **Delta Lake** e **Apache Iceberg**, utilizando **Apache Spark (PySpark)** como motor de processamento.

##  Documentação Completa

Toda a arquitetura, modelagem de dados, diagramas ER e a explicação técnica detalhada das operações transacionais foram documentadas e publicadas utilizando o MkDocs.

 **[Acesse a Documentação Pública do Projeto Aqui](https://NaiiiLr.github.io/ProjetoEngDeDados/)**

---

##  Objetivos do Projeto

* **Implementação de Transações ACID:** Garantir operações seguras de leitura/escrita simultâneas em um Data Lake.
* **Schema Enforcement:** Validação estrita do contrato de dados (DDL) para evitar ingestão de dados corrompidos.
* **Operações de DML Complexas:** Demonstração de inserções, atualizações, exclusões e sincronização atômica (`MERGE`/Upsert) sobre grandes volumes de dados.
* **Compute vs. Storage:** Desacoplamento da camada de processamento (Spark) da camada de armazenamento (Delta/Iceberg).

---

##  Estrutura do Repositório

O projeto está estruturado da seguinte forma:

```text
📦 ProjetoEngDeDados
 ┣ 📂 docs/                  # Arquivos Markdown e imagens da documentação
 ┣ 📜 apache_iceberg.ipynb   # Notebook PySpark com o laboratório Iceberg
 ┣ 📜 delta_lake.ipynb       # Notebook PySpark com o laboratório Delta Lake
 ┣ 📜 mkdocs.yml             # Configuração do site de documentação
 ┣ 📜 pyproject.toml         # Configuração do projeto e dependências (uv)
 ┣ 📜 uv.lock                # Arquivo de bloqueio para garantia de versões exatas
 ┣ 📜 .gitignore             # Arquivos ignorados pelo controle de versão
 ┗ 📜 README.md              # Este arquivo
```

##  Como Executar Localmente

### Pré-requisitos Técnicos

Para garantir a reprodutibilidade do ambiente, o projeto foi desenvolvido e testado sob as seguintes especificações:

* **Python 3.12.3:** Versão utilizada para o desenvolvimento dos scripts e notebooks.
* **Java JDK 21 (OpenJDK 21.0.10):** Necessário para a inicialização da JVM (Java Virtual Machine) utilizada pelo Apache Spark para o processamento distribuído.
* **uv:** Gerenciador de pacotes utilizado para garantir a velocidade e o isolamento das dependências.
* **Ambiente Linux / WSL (Ubuntu):** O projeto foi executado e validado em ambiente Ubuntu (POSIX) para garantir compatibilidade e evitar problemas nativos de gerenciamento de arquivos do Windows com o ecossistema Spark.
* **Dependências:** O projeto utiliza a arquitetura moderna do Python com [**`pyproject.toml`**](./pyproject.toml) e *uv.lock* para garantir versões exatas (Spark 3.5.x) e reprodutibilidade 100% fiel.

Versão Linux:

![Versão do Linux - Linux Ubuntu 24.04](/assets/linux.png)

### 1. Clonar o Repositório

```bash
git clone https://github.com/NaiiiLr/ProjetoEngDeDados.git

cd ProjetoEngDeDados
```

### 2. Configurar o Ambiente e Instalar Dependências

O projeto utiliza o `uv` em conjunto com o `pyproject.toml` e `uv.lock`. Com apenas um comando, o ambiente virtual é criado e as versões exatas das bibliotecas são instaladas:

```bash
# Cria a .venv e sincroniza as dependências exatas do projeto
uv sync

# Ativa o ambiente virtual
source .venv/bin/activate
```

### 3. Executar os Laboratórios (Jupyter Lab)

Com o ambiente ativado e as dependências instaladas, inicie a instância do Jupyter Lab para acessar e executar os testes interativos de comparação:

```bash
uv run jupyter lab
```

Após rodar o comando, o terminal exibirá uma URL (`ex: http://localhost:8888/lab?token=...`). Lá, você poderá abrir e executar as células dos arquivos `delta_lake.ipynb` e `apache_iceberg.ipynb`.

### 4. Executar a Documentação Localmente (MkDocs)

Toda a documentação técnica foi construída utilizando MkDocs. Se desejar rodar o site de documentação localmente na sua máquina, utilize o comando:

```bash
uv run mkdocs serve
```

Em seguida, acesse `http://localhost:8000` no seu navegador para explorar a arquitetura, diagramas e detalhes conceituais do projeto.
