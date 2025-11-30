# Exercício 2 - Descrição da Tech Stack

---

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ARQUITETURA DE DADOS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   purchase   │    │ product_item │    │  extra_info  │                   │
│  │   (eventos)  │    │   (eventos)  │    │   (eventos)  │                   │   
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                           │
│         └───────────────────┼───────────────────┘                           │
│                             ▼                                               │
│                    ┌────────────────┐                                       │
│                    │   Apache Spark │                                       │
│                    │   (PySpark)    │                                       │
│                    │   ETL Engine   │                                       │
│                    └────────┬───────┘                                       │
│                             │                                               │
│                             ▼                                               │
│                    ┌────────────────┐                                       │
│                    │  Data Lake     │                                       │
│                    │  (Parquet/     │                                       │
│                    │   Delta Lake)  │                                       │
│                    └────────┬───────┘                                       │
│                             │                                               │
│                             ▼                                               │
│                    ┌────────────────┐                                       │
│                    │ gmv_historico  │                                       │
│                    │  _subsidiaria  │                                       │
│                    │ (Tabela Final) │                                       │
│                    └────────┬───────┘                                       │
│                             │                                               │
│                             ▼                                               │
│                    ┌────────────────┐                                       │
│                    │   Consumidores │                                       │
│                    │  (BI, Análise) │                                       │
│                    └────────────────┘                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Modelo de Dados (Fonte)

Conforme diagrama dbdiagram:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MODELO DE DADOS - FONTE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐         ┌─────────────────────┐                    │
│  │      purchase       │         │    product_item     │                    │
│  ├─────────────────────┤         ├─────────────────────┤                    │
│  │ purchase_id (PK)    │         │ prod_item_id (PK)   │                    │
│  │ buyer_id            │         │ product_id          │                    │
│  │ prod_item_id (FK)───┼────────>│ item_quantity       │                    │
│  │ order_date          │         │ purchase_value ★    │ <- Valor do GMV   │
│  │ release_date ★      │         │ transaction_datetime│                   │
│  │ producer_id         │         │ transaction_date    │                    │
│  │ purchase_total_value│         └─────────────────────┘                    │
│  │ purchase_status     │                                                    │
│  │ transaction_datetime│                                                    │
│  │ transaction_date    │                                                    │
│  └──────────┬──────────┘                                                    │
│             │                                                               │
│             │ purchase_id                                                   │
│             ▼                                                               │
│  ┌─────────────────────┐                                                    │
│  │ purchase_extra_info │                                                    │
│  ├─────────────────────┤                                                    │
│  │ purchase_id (FK)    │                                                    │
│  │ subsidiary ★        │ <- Subsidiária para agrupamento                   │
│  │ transaction_datetime│                                                    │
│  │ transaction_date    │                                                    │
│  └─────────────────────┘                                                    │
│                                                                             │
│  ★ = Campos chave para cálculo do GMV                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Regra de Negócio: GMV

**GMV (Gross Merchandising Value)** = Soma de `product_item.purchase_value` para transações com `purchase.release_date` preenchido.

```sql
-- Lógica simplificada
select 
   p.order_date            reference_date,
   ei.subsidiary,
   sum(pi.purchase_value)  gmv_daily
from 
   purchase p
   join 
      product_item pi 
      on
         p.prod_item_id = pi.prod_item_id
   join
      purchase_extra_info ei 
      on
         p.purchase_id = ei.purchase_id
where 
   p.release_date is not null  -- Pagamento confirmado
group by 
   p.order_date, ei.subsidiary
```

---

## Componentes da Stack

### 1. Processamento de Dados

| Componente | Tecnologia | Justificativa |
|------------|------------|---------------|
| **Engine de ETL** | Apache Spark (PySpark) | Processamento distribuído, suporte a grandes volumes, integração nativa com formatos colunares |
| **Linguagem** | Python | Sintaxe clara, ampla adoção em Data Engineering, rica biblioteca de manipulação de dados |
| **Orquestração** | Apache Airflow | Agendamento D-1, monitoramento, retry automático, DAGs como código |

### 2. Armazenamento

| Componente | Tecnologia | Justificativa |
|------------|------------|---------------|
| **Data Lake** | Delta Lake / Apache Iceberg | Suporte a ACID, time travel, schema evolution, otimização automática |
| **Formato de Arquivo** | Parquet | Compressão eficiente, colunar, compatível com Spark e ferramentas de BI |
| **Particionamento** | Por `snapshot_date` | Otimiza consultas temporais e permite reprocessamento incremental |

### 3. Infraestrutura

| Componente | Tecnologia | Justificativa |
|------------|------------|---------------|
| **Cloud Provider** | AWS | Escalabilidade, managed services, integração com ecossistema |
| **Cluster Spark** | Databricks / EMR | Gerenciamento simplificado, auto-scaling, otimizações nativas |
| **Armazenamento** | S3 | Custo-benefício, durabilidade, integração com ferramentas analíticas |

### 4. Catálogo e Governança

| Componente | Tecnologia | Justificativa |
|------------|------------|---------------|
| **Metastore** | Hive Metastore / Unity Catalog | Centralização de metadados, controle de acesso |
| **Data Quality** | Great Expectations / dbt tests | Validação automatizada, documentação de regras |
| **Linhagem** | Apache Atlas / OpenLineage | Rastreabilidade end-to-end, compliance |

---

## Detalhamento Técnico

### Apache Spark (PySpark)

**Por que Spark?**
- **Processamento distribuído**: Capacidade de processar grandes volumes de dados de forma paralela
- **Lazy evaluation**: Otimização automática do plano de execução
- **APIs ricas**: DataFrame API, SQL, Structured Streaming
- **Ecossistema**: Integração com Delta Lake, Hive, Parquet, etc.

**Configurações recomendadas:**
```python
spark = (
    SparkSession.builder
    .appName("GMV_ETL_Hotmart")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "auto")
    .getOrCreate()
)
```

### Delta Lake

**Por que Delta Lake?**
- **ACID Transactions**: Garantia de consistência em operações concorrentes
- **Time Travel**: Navegação entre versões históricas dos dados
- **Schema Evolution**: Alterações de schema sem reprocessamento total
- **Optimize & Z-Order**: Otimização automática de arquivos pequenos

**Exemplo de uso:**
```python
# Escrita com Delta Lake
(
    df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("snapshot_date")
    .save("/path/to/gmv_historico")
)

# Time Travel
spark.read.format("delta").option("versionAsOf", 10).load("/path/to/gmv_historico")
```

### Apache Airflow

**Por que Airflow?**
- **DAGs como código**: Versionamento e revisão de pipelines
- **Agendamento flexível**: Suporte a cron expressions e sensors
- **Monitoramento**: Interface web, alertas, logs centralizados
- **Retry e backfill**: Recuperação automática de falhas

**Exemplo de DAG:**
```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gmv_etl_daily',
    default_args=default_args,
    schedule_interval='0 6 * * *',  # 6h da manhã (D-1)
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    run_gmv_etl = SparkSubmitOperator(
        task_id='run_gmv_etl',
        application='/path/to/exercicio_2_etl.py',
        conn_id='spark_default',
        conf={'spark.executor.memory': '4g'},
    )
```

---

## Modelagem SCD Type 2

### Conceito

O **Slowly Changing Dimension Type 2** é uma técnica de modelagem que:
- Preserva o histórico completo de alterações
- Permite navegação temporal (time travel)
- Garante imutabilidade do passado

### Implementação

```
┌─────────────────────────────────────────────────────────────────┐
│                    FLUXO SCD TYPE 2                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Novo registro chega                                         │
│     ┌─────────────────────────────────────────┐                 │
│     │ reference_date: 2023-01-20              │                 │
│     │ subsidiary: nacional                    │                 │
│     │ gmv_daily: 150.00                       │                 │
│     └─────────────────────────────────────────┘                 │
│                         │                                       │
│                         ▼                                       │
│  2. Verifica se existe registro corrente                        │
│     ┌─────────────────────────────────────────┐                 │
│     │ SELECT * FROM gmv_historico             │                 │
│     │ WHERE is_current = TRUE                 │                 │
│     │   AND reference_date = '2023-01-20'     │                 │
│     │   AND subsidiary = 'nacional'           │                 │
│     └─────────────────────────────────────────┘                 │
│                         │                                       │
│            ┌────────────┴────────────┐                          │
│            ▼                         ▼                          │
│     NÃO EXISTE                  EXISTE                          │
│     ┌──────────┐           ┌──────────────────┐                 │
│     │ Insere   │           │ 1. Fecha registro│                 │
│     │ novo     │           │    anterior      │                 │
│     │ registro │           │    (valid_to,    │                 │
│     │          │           │    is_current)   │                 │
│     └──────────┘           │ 2. Insere novo   │                 │
│                            │    registro      │                 │
│                            └──────────────────┘                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Campos de Controle

| Campo | Descrição | Uso |
|-------|-----------|-----|
| `valid_from` | Data de início da validade | Filtrar registros válidos em uma data |
| `valid_to` | Data de fim da validade (NULL = corrente) | Identificar versões antigas |
| `is_current` | Flag booleana de registro ativo | Consultas simplificadas de dados atuais |
| `snapshot_date` | Data do processamento | Particionamento e auditoria |

---

## Tratamento de Qualidade de Dados

### Regras Implementadas

1. **Deduplicação CDC**
   - Mantém apenas o registro mais recente por chave natural
   - Ordenação por `transaction_datetime` descendente
   - Chaves: `purchase_id` (purchase, extra_info), `prod_item_id` (product_item)

2. **Tratamento de Nulos**
   - `release_date = NULL` → Transação não liberada (não entra no GMV)
   - `subsidiary = NULL` → Substituído por "nao_informado"

3. **Chegada Assíncrona**
   - LEFT JOIN entre tabelas para não perder compras
   - Uso de `COALESCE` para priorizar dados mais recentes

4. **Validação de Integridade**
   - Query de verificação de duplicidades
   - Constraints de unicidade na surrogate key

---

## Vantagens da Solução

| Requisito | Como é Atendido |
|-----------|-----------------|
| **Imutabilidade do histórico** | SCD Type 2 com `valid_from`/`valid_to` |
| **Navegação temporal** | Filtros por `snapshot_date` e período de validade |
| **Registros correntes** | Flag `is_current = TRUE` |
| **Rastreabilidade diária** | Particionamento por `snapshot_date` |
| **Reprocessamento seguro** | Novos snapshots não alteram versões anteriores |
| **Facilidade de uso** | Consultas simples com `WHERE is_current = TRUE` |

---

## Escalabilidade e Performance

### Otimizações

1. **Particionamento por `snapshot_date`**
   - Pruning automático em consultas temporais
   - Reprocessamento incremental eficiente

2. **Formato Parquet com compressão Snappy**
   - Redução de 70-80% no tamanho dos dados
   - Leitura colunar otimizada

3. **Delta Lake Optimize**
   - Compactação automática de arquivos pequenos
   - Z-Order para otimizar filtros frequentes

4. **Spark Adaptive Query Execution**
   - Ajuste dinâmico de partições
   - Otimização de joins em runtime

---

## Monitoramento e Observabilidade

### Métricas Recomendadas

- **Latência do ETL**: Tempo de execução por etapa
- **Volume processado**: Registros por execução
- **Taxa de erro**: Falhas vs. sucessos
- **Data freshness**: Atraso em relação a D-1

### Alertas

- ETL não concluído até horário limite
- Variação anormal no volume de dados
- Falhas consecutivas de processamento

---

## Conclusão

A stack proposta combina tecnologias maduras e amplamente adotadas no mercado, garantindo:

- **Confiabilidade**: ACID transactions e histórico imutável
- **Escalabilidade**: Processamento distribuído com Spark
- **Manutenibilidade**: Código Python legível e bem documentado
- **Governança**: Linhagem, catálogo e controle de acesso
- **Facilidade de uso**: Consultas SQL simples para usuários finais

Esta arquitetura atende aos requisitos do desafio e está preparada para evoluir conforme as necessidades do negócio.
