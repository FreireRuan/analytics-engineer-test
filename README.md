# Desafio Técnico - Analytics Engineer

Solução desenvolvida para o teste técnico de Analytics Engineer

---

## 🎯 Navegação Rápida

| Exercício | Descrição | Arquivo |
|-----------|---------------|---------|
| **1 - SQL** | Queries analíticas | [`exercicio_1.sql`](./exercicio_1.sql) |
| **2 - ETL** | Script PySpark | [`exercicio_2_etl.py`](./exercicio_2_etl.py) |
| **2 - DDL** | Estrutura da tabela | [`exercicio_2_ddl.sql`](./exercicio_2_ddl.sql) |
| **2 - Dataset** | Exemplo populado | [`exercicio_2_dataset_exemplo.md`](./exercicio_2_dataset_exemplo.md) |
| **2 - Queries** | Consultas GMV | [`exercicio_2_queries_gmv.sql`](./exercicio_2_queries_gmv.sql) |
| **2 - Tech Stack** | Arquitetura | [`exercicio_2_tech_stack.md`](./exercicio_2_tech_stack.md) |

---

## 📝 Exercício 1 - SQL

**Arquivo:** [`exercicio_1.sql`](./exercicio_1.sql)

Queries para responder às perguntas do desafio:

| # | Pergunta | Técnica utilizada |
|---|----------|-------------------|
| 1 | Quais são os 50 maiores produtores em faturamento de 2021? | CTE + `ROW_NUMBER()` |
| 2 | Quais são os 2 produtos que mais faturaram de cada produtor? | CTE + `ROW_NUMBER()` com `PARTITION BY` |

---

## 🔄 Exercício 2 - Modelagem e Desenvolvimento GMV

### Contexto

O objetivo é calcular o **GMV (Gross Merchandising Value)** diário por subsidiária, com uma modelagem que:
- Preserve o histórico de forma imutável
- Permita navegação temporal
- Facilite a recuperação de dados correntes

### Regra de Negócio

```
GMV = SUM(product_item.purchase_value)
WHERE purchase.release_date IS NOT NULL
```

### Entregáveis

#### 1️⃣ Script ETL → [`exercicio_2_etl.py`](./exercicio_2_etl.py)

Pipeline em Python/PySpark que:
- Lê eventos CDC das tabelas `purchase`, `product_item` e `purchase_extra_info`
- Aplica deduplicação mantendo registros mais recentes
- Calcula GMV diário por subsidiária
- Implementa versionamento SCD Type 2

#### 2️⃣ DDL da Tabela Final → [`exercicio_2_ddl.sql`](./exercicio_2_ddl.sql)

Estrutura da tabela `gmv_historico_subsidiaria` com:
- Campos de negócio (reference_date, subsidiary, gmv_daily)
- Campos de controle SCD Type 2 (valid_from, valid_to, is_current)
- Particionamento por snapshot_date

#### 3️⃣ Dataset Exemplo → [`exercicio_2_dataset_exemplo.md`](./exercicio_2_dataset_exemplo.md)

Demonstração completa com:
- Dados de entrada (conforme PDF do desafio)
- Lógica de deduplicação aplicada
- Resultado final da tabela GMV

#### 4️⃣ Queries GMV → [`exercicio_2_queries_gmv.sql`](./exercicio_2_queries_gmv.sql)

Consultas SQL incluindo:
- **Query principal**: GMV diário por subsidiária (dados correntes)
- Navegação temporal entre snapshots
- Validação de integridade

#### 5️⃣ Tech Stack → [`exercicio_2_tech_stack.md`](./exercicio_2_tech_stack.md)

Documentação da arquitetura:
- Modelo de dados fonte
- Componentes da stack (Spark, Delta Lake, Airflow)
- Justificativas técnicas

---

## 🏗️ Arquitetura Resumida

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  purchase   │    │product_item │    │ extra_info  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          ▼
                 ┌────────────────┐
                 │  Apache Spark  │
                 │   (PySpark)    │
                 └────────┬───────┘
                          ▼
                 ┌────────────────┐
                 │gmv_historico_  │
                 │  subsidiaria   │
                 └────────────────┘
```

---

## 📊 Modelagem SCD Type 2

| Requisito | Como foi implementado |
|-----------|----------------------|
| Imutabilidade | Campos `valid_from` e `valid_to` |
| Navegação temporal | Filtros por período de validade |
| Dados correntes | Flag `is_current = TRUE` |
| Rastreabilidade | Partição por `snapshot_date` |

---

## 🛠️ Tech Stack

| Componente | Tecnologia |
|------------|------------|
| ETL Engine | Apache Spark (PySpark) |
| Linguagem | Python |
| Armazenamento | Delta Lake / Parquet |
| Orquestração | Apache Airflow |
| Cloud | AWS |

Detalhes em [`exercicio_2_tech_stack.md`](./exercicio_2_tech_stack.md).
