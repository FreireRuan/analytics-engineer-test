"""
EXERCÍCIO 2 - ETL para GMV

GMV = soma do purchase_value para transações com release_date preenchido
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, date, timedelta


# =============================================================================
# CONFIGURAÇÃO DO SPARK
# =============================================================================

def criar_sessao_spark():
    return (
        SparkSession.builder
        .appName("GMV_ETL")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# =============================================================================
# FUNÇÕES DE PROCESSAMENTO
# =============================================================================

def deduplicar_por_chave(df, chave, coluna_ordenacao="transaction_datetime"):
    """
    Mantém apenas o registro mais recente para cada chave.
    Útil para tratar dados CDC onde podem existir múltiplas versões.
    """
    window = Window.partitionBy(chave).orderBy(F.col(coluna_ordenacao).desc())
    
    return (
        df
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def consolidar_tabelas(purchase_df, product_item_df, extra_info_df):
    """
    Junta as 3 tabelas fonte para criar uma visão consolidada.
    
    Relacionamentos:
    - purchase.prod_item_id -> product_item.prod_item_id
    - purchase.purchase_id -> purchase_extra_info.purchase_id
    """
    # Deduplica cada tabela
    purchase = deduplicar_por_chave(purchase_df, "purchase_id")
    product_item = deduplicar_por_chave(product_item_df, "prod_item_id")
    extra_info = deduplicar_por_chave(extra_info_df, "purchase_id")
    
    # Faz os joins
    consolidado = (
        purchase.alias("p")
        .join(
            product_item.alias("pi"),
            F.col("p.prod_item_id") == F.col("pi.prod_item_id"),
            "left"
        )
        .join(
            extra_info.alias("ei"),
            F.col("p.purchase_id") == F.col("ei.purchase_id"),
            "left"
        )
        .select(
            F.col("p.purchase_id"),
            F.col("p.order_date"),
            F.col("p.release_date"),
            F.col("pi.purchase_value"),
            F.coalesce(F.col("ei.subsidiary"), F.lit("nao_informado")).alias("subsidiary")
        )
    )
    
    return consolidado


def calcular_gmv(df_consolidado):
    """
    Calcula o GMV diário por subsidiária.
    
    Regra: Só entra no GMV se release_date estiver preenchido.
    """
    # Filtra transações com pagamento confirmado
    transacoes_validas = df_consolidado.filter(F.col("release_date").isNotNull())
    
    # Agrupa por data e subsidiária
    gmv_diario = (
        transacoes_validas
        .groupBy(
            F.col("order_date").alias("reference_date"),
            F.col("subsidiary")
        )
        .agg(
            F.sum("purchase_value").alias("gmv_daily"),
            F.count("purchase_id").alias("transaction_count")
        )
    )
    
    # Calcula acumulado por subsidiária
    window_acumulado = (
        Window
        .partitionBy("subsidiary")
        .orderBy("reference_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    
    gmv_com_acumulado = gmv_diario.withColumn(
        "gmv_accumulated",
        F.sum("gmv_daily").over(window_acumulado)
    )
    
    return gmv_com_acumulado


def adicionar_campos_scd2(df_gmv, snapshot_date):
    """
    Adiciona campos de controle para SCD Type 2.
    
    Campos:
    - sk_gmv: chave única do registro
    - snapshot_date: data do processamento
    - valid_from/valid_to: período de validade
    - is_current: indica se é o registro atual
    """
    agora = datetime.now()
    
    return (
        df_gmv
        .withColumn(
            "sk_gmv",
            F.concat_ws("_",
                F.col("reference_date").cast("string"),
                F.col("subsidiary"),
                F.lit(str(snapshot_date))
            )
        )
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .withColumn("valid_from", F.lit(snapshot_date))
        .withColumn("valid_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("created_at", F.lit(agora))
        .withColumn("updated_at", F.lit(agora))
    )


def salvar_resultado(df, caminho_saida):
    """Salva o resultado particionado por snapshot_date."""
    (
        df
        .write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .parquet(caminho_saida)
    )
    print(f"Dados salvos em: {caminho_saida}")


# =============================================================================
# DADOS DE EXEMPLO (conforme PDF do desafio)
# =============================================================================

def criar_dados_exemplo(spark):
    """Cria DataFrames com os dados do desafio."""
    
    # Tabela purchase
    purchase_data = [
        ("2023-01-20 22:00:00", "2023-01-20", 55, 5, "2023-01-20", "2023-01-20"),
        ("2023-01-26 00:01:00", "2023-01-26", 56, 746520, "2023-01-25", None),
        ("2023-02-05 10:00:00", "2023-02-05", 69, 5, "2023-01-20", "2023-01-20"),
        ("2023-02-26 03:00:00", "2023-02-26", 69, 18, "2023-02-26", "2023-02-28"),
        ("2023-07-15 09:00:00", "2023-07-15", 55, 5, "2023-01-20", "2023-03-01"),
    ]
    
    purchase_df = spark.createDataFrame(
        purchase_data,
        ["transaction_datetime", "transaction_date", "purchase_id", 
         "prod_item_id", "order_date", "release_date"]
    )
    
    # Tabela product_item (com purchase_value para GMV)
    product_item_data = [
        ("2023-01-20 21:00:00", "2023-01-20", 5, 150.00),
        ("2023-01-25 20:00:00", "2023-01-25", 746520, 200.00),
        ("2023-02-26 02:00:00", "2023-02-26", 18, 350.00),
    ]
    
    product_item_df = spark.createDataFrame(
        product_item_data,
        ["transaction_datetime", "transaction_date", "prod_item_id", "purchase_value"]
    )
    
    # Tabela purchase_extra_info
    extra_info_data = [
        ("2023-01-23 00:05:00", "2023-01-23", 55, "nacional"),
        ("2023-01-25 23:59:59", "2023-01-25", 56, "internacional"),
        ("2023-02-28 01:10:00", "2023-02-28", 69, "nacional"),
        ("2023-03-12 07:00:00", "2023-03-12", 69, "internacional"),
    ]
    
    extra_info_df = spark.createDataFrame(
        extra_info_data,
        ["transaction_datetime", "transaction_date", "purchase_id", "subsidiary"]
    )
    
    return purchase_df, product_item_df, extra_info_df


# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

def executar_etl():
    """Executa o pipeline ETL completo."""
    
    # 1. Cria sessão Spark
    spark = criar_sessao_spark()
    
    # 2. Carrega dados de exemplo
    purchase_df, product_item_df, extra_info_df = criar_dados_exemplo(spark)
    
    print("=== DADOS DE ENTRADA ===")
    print("\n>> Purchase:")
    purchase_df.show(truncate=False)
    
    print("\n>> Product Item:")
    product_item_df.show(truncate=False)
    
    print("\n>> Purchase Extra Info:")
    extra_info_df.show(truncate=False)
    
    # 3. Consolida as tabelas
    consolidado = consolidar_tabelas(purchase_df, product_item_df, extra_info_df)
    
    print("\n=== DADOS CONSOLIDADOS ===")
    consolidado.show(truncate=False)
    
    # 4. Calcula GMV
    gmv = calcular_gmv(consolidado)
    
    print("\n=== GMV DIÁRIO POR SUBSIDIÁRIA ===")
    gmv.show(truncate=False)
    
    # 5. Adiciona campos SCD Type 2
    data_processamento = date.today()
    snapshot_date = data_processamento - timedelta(days=1)
    
    gmv_final = adicionar_campos_scd2(gmv, snapshot_date)
    
    print("\n=== TABELA FINAL (GMV HISTÓRICO) ===")
    gmv_final.show(truncate=False)
    
    # 6. Encerra Spark
    spark.stop()
    
    print("\nETL concluído com sucesso!")


if __name__ == "__main__":
    executar_etl()
