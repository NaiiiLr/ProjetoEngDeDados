# Apache Iceberg: Open Table Format

O **Apache Iceberg** é um formato de tabela aberto e de alto desempenho para conjuntos de dados analíticos gigantescos. Ele resolve problemas de lentidão e consistência presentes em Data Lakes tradicionais, substituindo o mapeamento de diretórios do Hadoop por um rastreamento rigoroso em nível de arquivo.

---

## Diferenciais Técnicos do Iceberg

O Iceberg opera com uma arquitetura focada em metadados, o que proporciona escalabilidade massiva e confiabilidade:

* **Transações ACID e Isolamento Snapshot:** Leitores e escritores operam simultaneamente sem interferência. As consultas sempre veem um instantâneo (snapshot) consistente dos dados.
* **Árvore de Metadados:** Em vez de listar diretórios pesados, o Iceberg usa uma estrutura hierárquica (Catalog > Metadata > Manifest List > Manifests) que permite executar consultas em petabytes de dados em milissegundos.
* **Schema Evolution (In-Place):** Permite adicionar, remover ou renomear colunas de forma instantânea sem precisar reescrever os arquivos de dados físicos.
* **Hidden Partitioning:** Abstrai a complexidade do particionamento, evitando que usuários façam consultas lentas por desconhecerem as chaves de partição físicas.

---

## Laboratório de Manipulação de Dados (DML)

Abaixo, o script de execução utilizado no ambiente Spark. Repare que, diferentemente do Delta (que pode usar caminhos de pasta), **o Iceberg exige a referência ao namespace e catálogo** (ex: `local.db.produtos`) para que o rastreamento de metadados funcione corretamente.

### 0. Mostrar Estado Inicial 

```python
print("--- TABELA ORIGINAL ---")
spark.sql("SELECT * FROM local.db.produtos ORDER BY id_produto").show()
```
Resultado esperado:

```
+----------+----+------------+-----+-------+
|id_produto|nome|id_categoria|preco|estoque|
+----------+----+------------+-----+-------+
+----------+----+------------+-----+-------+
```

### 1. INSERT: Adicionando múltiplos produtos novos 

```python
spark.sql("""
    INSERT INTO local.db.produtos VALUES
    (104, 'Smartwatch Pro', 1, 1500.00, 50),
    (105, 'Teclado Mecânico RGB', 2, 450.00, 120),
    (106, 'Monitor Ultrawide 34"', 2, 2800.00, 30)
""")

print("--- DEPOIS DO INSERT (3 Novos produtos adicionados) ---")
spark.sql("SELECT * FROM local.db.produtos ORDER BY id_produto").show()
```

Resultado esperado:

```
+----------+--------------------+------------+-------+-------+
|id_produto|                nome|id_categoria|  preco|estoque|
+----------+--------------------+------------+-------+-------+
|       104|      Smartwatch Pro|           1| 1500.0|     50|
|       105|Teclado Mecânico RGB|           2|  450.0|    120|
|       106|Monitor Ultrawide 34|           2| 2800.0|     30|
+----------+--------------------+------------+-------+-------+
```

### 2. UPDATE: Atualização de Preço e Estoque

```python
spark.sql("""
    UPDATE local.db.produtos
    SET preco = 6200.00, 
        estoque = estoque - 5
    WHERE id_produto = 106
""")

print("--- DEPOIS DO UPDATE (Monitor: Preço ajustado e Estoque reduzido) ---")
spark.sql("SELECT * FROM local.db.produtos ORDER BY id_produto").show()
```

Resultado esperado:

```
+----------+--------------------+------------+-------+-------+
|id_produto|                nome|id_categoria|  preco|estoque|
+----------+--------------------+------------+-------+-------+
|       104|      Smartwatch Pro|           1| 1500.0|     50|
|       105|Teclado Mecânico RGB|           2|  450.0|    120|
|       106|Monitor Ultrawide 34|           2| 6200.0|     25|
+----------+--------------------+------------+-------+-------+
```

### 3. DELETE: Exclusão Física de Registro

```python
spark.sql("""
    DELETE FROM local.db.produtos
    WHERE id_produto = 105
""")

print("--- DEPOIS DO DELETE (Produto 105 removido) ---")
spark.sql("SELECT * FROM local.db.produtos ORDER BY id_produto").show()
```

Resultado esperado:

```
+----------+--------------------+------------+-------+-------+
|id_produto|                nome|id_categoria|  preco|estoque|
+----------+--------------------+------------+-------+-------+
|       104|      Smartwatch Pro|           1| 1500.0|     50|
|       106|Monitor Ultrawide 34|           2| 6200.0|     25|
+----------+--------------------+------------+-------+-------+
```

### 4. MERGE (UPSERT): Sincronização Atômica

```python
spark.sql("""
    MERGE INTO local.db.produtos destino
    USING (
        SELECT 104 as id_produto, 'Smartwatch Pro V2' as nome, 1 as id_categoria, 1650.00 as preco, 80 as estoque
        UNION ALL
        SELECT 107 as id_produto, 'Mouse Sem Fio', 2 as id_categoria, 180.00 as preco, 200 as estoque
    ) origem
    ON destino.id_produto = origem.id_produto
    
    WHEN MATCHED THEN
        UPDATE SET 
            destino.nome = origem.nome, 
            destino.preco = origem.preco, 
            destino.estoque = origem.estoque
            
    WHEN NOT MATCHED THEN
        INSERT (id_produto, nome, id_categoria, preco, estoque)
        VALUES (origem.id_produto, origem.nome, origem.id_categoria, origem.preco, origem.estoque)
""")

print("--- DEPOIS DO MERGE (Sincronização Final Concluída) ---")
spark.sql("SELECT * FROM local.db.produtos ORDER BY id_produto").show()
```

Resultado esperado:

```
+----------+--------------------+------------+-------+-------+
|id_produto|                nome|id_categoria|  preco|estoque|
+----------+--------------------+------------+-------+-------+
|       104|   Smartwatch Pro V2|           1| 1650.0|     80|
|       106|Monitor Ultrawide 34|           2| 6200.0|     25|
|       107|       Mouse Sem Fio|           2|  180.0|    200|
+----------+--------------------+------------+-------+-------+
```