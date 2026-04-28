#  Delta Lake: Reliability & Performance

O **Delta Lake** é uma camada de armazenamento de código aberto que traz confiabilidade aos Data Lakes. Ele resolve o problema de inconsistência de dados através de um protocolo de transações rigoroso, transformando arquivos Parquet em tabelas inteligentes com suporte total a SQL.

---

###  Diferenciais Técnicos do Delta

O Delta Lake não é apenas um formato de arquivo; é um sistema de gerenciamento de dados que oferece:

* **Transações ACID:** Garante que as operações (como vendas e atualizações de estoque) sejam concluídas com sucesso ou falhem totalmente, sem deixar dados corrompidos.
* **Log de Transações (_delta_log):** Cada alteração gera um arquivo JSON de log. Isso permite que o Spark saiba exatamente qual é a versão mais recente dos dados.
* **Schema Enforcement:** Impede que dados "sujos" ou com colunas faltando entrem na tabela, forçando o cumprimento do contrato definido no DDL.
* **Time Travel:** Capacidade de consultar versões anteriores da tabela para auditoria ou recuperação de desastres.

---

###  Laboratório de Manipulação de Dados (DML)

Abaixo, apresentamos o script de execução utilizado no ambiente Spark para gerenciar o fluxo do e-commerce. As operações incluem inserções em lote, atualizações condicionais, exclusões e a sincronização via `MERGE`.

### 0. Mostrar Estado Inicial 

```python
print("--- TABELA ORIGINAL ---")
spark.sql("SELECT * FROM produtos ORDER BY id_produto").show()
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
    INSERT INTO produtos VALUES
    (104, 'Smartwatch Pro', 1, 1500.00, 50),
    (105, 'Teclado Mecânico RGB', 2, 450.00, 120),
    (106, 'Monitor Ultrawide 34"', 2, 2800.00, 30)
""")

print("--- DEPOIS DO INSERT (3 Novos produtos adicionados) ---")
spark.sql("SELECT * FROM produtos ORDER BY id_produto").show()
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
    UPDATE produtos
    SET preco = 6200.00, 
        estoque = estoque - 5
    WHERE id_produto = 102
""")

print("--- DEPOIS DO UPDATE (Notebook: Preço ajustado e Estoque reduzido) ---")
spark.sql("SELECT * FROM produtos ORDER BY id_produto").show()
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
    DELETE FROM produtos
    WHERE id_produto = 103
""")

print("--- DEPOIS DO DELETE (Produto 103 removido) ---")
spark.sql("SELECT * FROM produtos ORDER BY id_produto").show()
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
    MERGE INTO produtos destino
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
spark.sql("SELECT * FROM produtos ORDER BY id_produto").show()
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