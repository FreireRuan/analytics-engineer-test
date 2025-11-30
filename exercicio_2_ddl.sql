-- ============================================================================
-- EXERCÍCIO 2 - DDL (Data Definition Language)
-- Tabela Final: GMV Histórico por Subsidiária
-- ============================================================================

-- ============================================================================
-- DDL PARA HIVE/SPARK SQL
-- ============================================================================

CREATE TABLE IF NOT EXISTS analytics.gmv_historico_subsidiaria (
    -- Chave surrogate única para cada registro
    sk_gmv                  STRING          COMMENT 'Surrogate key: reference_date_subsidiary_snapshot_date',
    
    -- Dimensões de negócio
    reference_date          DATE            COMMENT 'Data de referência do GMV (order_date da compra)',
    subsidiary              STRING          COMMENT 'Subsidiária (nacional, internacional, nao_informado)',
    
    -- Métricas de GMV
    gmv_daily               FLOAT           COMMENT 'GMV do dia (soma de product_item.purchase_value das transações liberadas)',
    gmv_accumulated         FLOAT           COMMENT 'GMV acumulado até a data de referência por subsidiária',
    transaction_count       INT             COMMENT 'Quantidade de transações no dia',
    
    -- Controle de versionamento (SCD Type 2)
    snapshot_date           DATE            COMMENT 'Data do snapshot/processamento (D-1)',
    valid_from              DATE            COMMENT 'Data de início da validade do registro',
    valid_to                DATE            COMMENT 'Data de fim da validade (NULL = registro corrente)',
    is_current              BOOLEAN         COMMENT 'Flag indicando se é o registro corrente (TRUE/FALSE)',
    
    -- Auditoria
    created_at              TIMESTAMP       COMMENT 'Data/hora de criação do registro',
    updated_at              TIMESTAMP       COMMENT 'Data/hora da última atualização'
)
COMMENT 'Tabela histórica de GMV diário por subsidiária com versionamento SCD Type 2. GMV = soma de product_item.purchase_value para compras com release_date preenchido.'
PARTITIONED BY (snapshot_date DATE)
STORED AS PARQUET
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'transactional' = 'false'
);

-- ============================================================================
-- DESCRIÇÃO DOS CAMPOS E REGRAS DE NEGÓCIO
-- ============================================================================

/*
DICIONÁRIO DE DADOS:

| Campo             | Tipo    | Descrição                                                      |
|-------------------|---------|----------------------------------------------------------------|
| sk_gmv            | STRING  | Chave surrogate: reference_date + subsidiary + snapshot_date  |
| reference_date    | DATE    | Data de referência do GMV (baseada no order_date da compra)   |
| subsidiary        | STRING  | Subsidiária da transação (nacional, internacional, etc)       |
| gmv_daily         | FLOAT   | GMV do dia = SUM(product_item.purchase_value)                 |
| gmv_accumulated   | FLOAT   | GMV acumulado por subsidiária até a data de referência        |
| transaction_count | INT     | Quantidade de transações que compõem o GMV do dia             |
| snapshot_date     | DATE    | Data do snapshot (D-1). Usado como partição                   |
| valid_from        | DATE    | Data de início da validade do registro (SCD Type 2)           |
| valid_to          | DATE    | Data de fim da validade. NULL indica registro corrente        |
| is_current        | BOOLEAN | Flag de fácil identificação de registros correntes            |
| created_at        | TIMESTAMP | Momento de criação do registro (auditoria)                  |
| updated_at        | TIMESTAMP | Momento da última atualização (auditoria)                   |


REGRAS DE NEGÓCIO:

1. GMV = soma de product_item.purchase_value para transações com release_date preenchido
   
2. Relacionamentos:
   - purchase.prod_item_id -> product_item.prod_item_id (para obter purchase_value)
   - purchase.purchase_id -> purchase_extra_info.purchase_id (para obter subsidiary)

3. Filtros:
   - Apenas transações com release_date IS NOT NULL entram no GMV
   - Transações sem release_date (pagamento não confirmado) são ignoradas


PARTICIONAMENTO:
- A tabela é particionada por snapshot_date para otimizar:
  1. Consultas temporais (navegação entre versões)
  2. Manutenção de dados (retenção, compactação)
  3. Reprocessamento incremental


VERSIONAMENTO SCD TYPE 2:
- Cada alteração cria uma nova versão do registro
- O registro anterior tem valid_to preenchido e is_current = FALSE
- O novo registro tem valid_to = NULL e is_current = TRUE
- Isso garante imutabilidade do histórico mesmo com reprocessamento
*/
