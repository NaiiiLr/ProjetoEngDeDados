#  Apache Spark: O Motor de Processamento Distribuído

O **Apache Spark** é o coração desta arquitetura. Ele foi projetado para superar as limitações do antigo MapReduce, realizando processamento **em memória** (In-Memory), o que o torna mais rápido para cargas de trabalho analíticas.

---

###  Arquitetura
O Spark opera em um modelo de computação distribuída. Quando você executa um comando no seu notebook, acontece uma divisão de tarefas:

* **Driver Program:** É o "cérebro". Ele converte seu código Python em tarefas físicas e agenda a execução.
* **Cluster Manager:** Gerencia os recursos (CPU/Memória).
* **Executors:** São os "operários". Eles ficam nos nós do cluster, executando as tarefas e armazenando os dados em cache.


---

###  Por que ele é tão rápido? (Catalyst & Tungsten)

O Spark não apenas "roda" SQL; ele o otimiza antes de tocar no disco através de dois motores internos:

1.  **Catalyst Optimizer:** Ele pega o código `spark.sql("SELECT...")` e cria vários planos de execução. Ele escolhe o caminho matematicamente mais eficiente para chegar ao resultado (ex: filtrando dados antes de fazer Joins).
2.  **Project Tungsten:** Otimiza o uso de memória e CPU, removendo o overhead do Java (JVM) e operando quase diretamente no hardware.

---

###  Abstrações de Dados: Do RDD ao DataFrame

Embora o Spark tenha começado com RDDs, neste projeto utilizamos exclusivamente **DataFrames**.
* **DataFrames:** São tabelas estruturadas com colunas e tipos definidos. É essa estrutura que permite ao Spark aplicar o **Schema Enforcement**.

---

###  Configuração Técnica da Sessão

Para este projeto de E-commerce, a inicialização da `SparkSession` é o momento onde "ensinamos" ao Spark como ler os formatos modernos. Sem estas linhas, o Spark veria o Delta e o Iceberg apenas como pastas cheias de arquivos Parquet ilegíveis.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Ecommerce_Data_Platform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "warehouse_ecommerce") \
    .getOrCreate()
```