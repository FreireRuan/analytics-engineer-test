"""
============================================================================
EXERCÍCIO 2 - ETL para GMV (Gross Merchandising Value)
============================================================================

Objetivo: Construir um ETL que calcule o GMV diário por subsidiária,
garantindo modelagem histórica e imutável.

Requisitos:
- GMV = soma do purchase_value (de product_item) para transações com release_date preenchido
- Modelagem histórica e imutável
- Rastreabilidade diária
- Fácil recuperação de registros correntes
- Suporte a reprocessamento sem alterar histórico

Tech Stack: Python + PySpark
============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    FloatType, DateType, TimestampType, BooleanType, IntegerType
)
from datetime import datetime, date
from typing import Optional


def create_spark_session() -> SparkSession:
    """criar uma sessão Spark"""
    return (
        SparkSession.builder
        .appName("GMV_ETL_Hotmart")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

# ============================================================================
# SCHEMAS DAS TABELAS DE EVENTOS (CDC) - seguindo diagrama dbdiagram
# ============================================================================

# Schema: purchase (eventos de compra)
PURCHASE_SCHEMA = StructType([
    StructField("transaction_datetime", TimestampType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("purchase_id", LongType(), False),
    StructField("buyer_id", LongType(), True),
    StructField("prod_item_id", LongType(), True),
    StructField("order_date", DateType(), True),
    StructField("release_date", DateType(), True),
    StructField("producer_id", LongType(), True),
    StructField("purchase_partition", LongType(), True),
    StructField("prod_item_partition", LongType(), True),
    StructField("purchase_total_value", FloatType(), True),
    StructField("purchase_status", StringType(), True),
])

# Schema: product_item (dados do item de compra)
PRODUCT_ITEM_SCHEMA = StructType([
    StructField("transaction_datetime", TimestampType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("prod_item_id", LongType(), False),
    StructField("prod_item_partition", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("item_quantity", IntegerType(), True),
    StructField("purchase_value", FloatType(), True),  # Valor usado para calcular GMV
])

# Schema: purchase_extra_info (informações extras da compra)
PURCHASE_EXTRA_INFO_SCHEMA = StructType([
    StructField("transaction_datetime", TimestampType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("purchase_id", LongType(), False),
    StructField("purchase_partition", LongType(), True),
    StructField("subsidiary", StringType(), True),
])

# Schema: Tabela final de GMV histórico
GMV_HISTORICO_SCHEMA = StructType([
    StructField("sk_gmv", StringType(), False),           # Surrogate key
    StructField("reference_date", DateType(), False),     # Data de referência do GMV
    StructField("subsidiary", StringType(), False),       # Subsidiária
    StructField("gmv_daily", FloatType(), True),          # GMV do dia
    StructField("gmv_accumulated", FloatType(), True),    # GMV acumulado
    StructField("transaction_count", IntegerType(), True),# Qtd de transações
    StructField("snapshot_date", DateType(), False),      # Data do snapshot (D-1)
    StructField("valid_from", DateType(), False),         # Início da validade
    StructField("valid_to", DateType(), True),            # Fim da validade (NULL = corrente)
    StructField("is_current", BooleanType(), False),      # Flag de registro corrente
    StructField("created_at", TimestampType(), False),    # Data de criação
    StructField("updated_at", TimestampType(), False),    # Data de atualização
])

class GMVProcessor:
    """
    Processador de GMV
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.processing_date = date.today()
        
    def set_processing_date(self, processing_date: date):
        """Define a data de processamento"""
        self.processing_date = processing_date
        
    def read_purchase_events(self, path: str):
        """Lê eventos de purchase do data lake."""
        return self.spark.read.schema(PURCHASE_SCHEMA).parquet(path)
    
    def read_product_item_events(self, path: str):
        """Lê eventos de product_item do data lake."""
        return self.spark.read.schema(PRODUCT_ITEM_SCHEMA).parquet(path)
    
    def read_purchase_extra_info_events(self, path: str):
        """Lê eventos de purchase_extra_info do data lake."""
        return self.spark.read.schema(PURCHASE_EXTRA_INFO_SCHEMA).parquet(path)

    def get_latest_records(self, df, partition_cols: list, order_col: str = "transaction_datetime"):
        """
        Obtém os registros mais recentes para cada chave de partição.
        
        Implementa a lógica de deduplicação para CDC, mantendo apenas
        o registro mais recente baseado em transaction_datetime.
        
        Args:
            df: DataFrame com eventos CDC
            partition_cols: Colunas que formam a chave natural
            order_col: Coluna para ordenação (mais recente primeiro)
            
        Returns:
            DataFrame com apenas os registros mais recentes
        """
        window_spec = Window.partitionBy(partition_cols).orderBy(F.col(order_col).desc())
        
        return (
            df
            .withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )

    def process_source_tables(self, purchase_df, product_item_df, extra_info_df):
        """
        Processa e consolida as tabelas fonte.
        
        Aplica deduplicação CDC e realiza o join entre as tabelas:
        - purchase (chave: purchase_id) -> contém release_date, order_date
        - product_item (chave: prod_item_id) -> contém purchase_value (GMV)
        - purchase_extra_info (chave: purchase_id) -> contém subsidiary
        
        Join: purchase.prod_item_id = product_item.prod_item_id
        
        Args:
            purchase_df: DataFrame de eventos de compra
            product_item_df: DataFrame de eventos de item de produto
            extra_info_df: DataFrame de informações extras
            
        Returns:
            DataFrame consolidado com informações de compra
        """
        # Deduplica cada fonte mantendo o registro mais recente
        latest_purchase = self.get_latest_records(purchase_df, ["purchase_id"])
        latest_product_item = self.get_latest_records(product_item_df, ["prod_item_id"])
        latest_extra_info = self.get_latest_records(extra_info_df, ["purchase_id"])
        
        # Join das tabelas conforme relacionamento do diagrama:
        # purchase.prod_item_id -> product_item.prod_item_id
        # purchase.purchase_id -> purchase_extra_info.purchase_id
        consolidated = (
            latest_purchase
            .alias("p")
            .join(
                latest_product_item.alias("pi"),
                F.col("p.prod_item_id") == F.col("pi.prod_item_id"),
                "left"
            )
            .join(
                latest_extra_info.alias("ei"),
                F.col("p.purchase_id") == F.col("ei.purchase_id"),
                "left"
            )
            .select(
                F.col("p.purchase_id"),
                F.col("p.buyer_id"),
                F.col("p.prod_item_id"),
                F.col("p.order_date"),
                F.col("p.release_date"),
                F.col("p.producer_id"),
                # Valor do GMV vem de product_item.purchase_value
                F.col("pi.purchase_value"),
                # Trata subsidiária NULL como "nao_informado"
                F.coalesce(
                    F.col("ei.subsidiary"), 
                    F.lit("nao_informado")
                ).alias("subsidiary"),
                F.greatest(
                    F.col("p.transaction_datetime"),
                    F.col("pi.transaction_datetime"),
                    F.col("ei.transaction_datetime")
                ).alias("last_update_datetime")
            )
        )
        
        return consolidated

    def calculate_gmv(self, consolidated_df):
        """
        Calcula o GMV diário por subsidiária.
        
        GMV = soma do purchase_value (de product_item) para transações 
        com release_date preenchido (pagamento efetuado e não cancelado).
        
        Args:
            consolidated_df: DataFrame consolidado de compras
            
        Returns:
            DataFrame com GMV diário por subsidiária
        """
        # Filtra apenas transações com pagamento liberado (release_date preenchido)
        valid_transactions = consolidated_df.filter(
            F.col("release_date").isNotNull()
        )
        
        # Agrupa por data de referência (order_date) e subsidiária
        gmv_daily = (
            valid_transactions
            .groupBy(
                F.col("order_date").alias("reference_date"),
                F.col("subsidiary")
            )
            .agg(
                F.sum("purchase_value").alias("gmv_daily"),
                F.count("purchase_id").alias("transaction_count")
            )
        )
        
        # Calcula GMV acumulado por subsidiária
        window_accumulated = (
            Window
            .partitionBy("subsidiary")
            .orderBy("reference_date")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        
        gmv_with_accumulated = gmv_daily.withColumn(
            "gmv_accumulated",
            F.sum("gmv_daily").over(window_accumulated)
        )
        
        return gmv_with_accumulated

    def apply_scd_type2(self, current_gmv_df, existing_history_df, snapshot_date: date):
        """
        Aplica SCD Type 2 para manter histórico imutável.
        
        Garante que:
        - O passado não seja alterado mesmo com reprocessamento
        - Novos valores criam novas versões
        - Registros correntes são facilmente identificáveis
        
        Args:
            current_gmv_df: DataFrame com GMV calculado atual
            existing_history_df: DataFrame com histórico existente
            snapshot_date: Data do snapshot (D-1)
            
        Returns:
            DataFrame com histórico atualizado
        """
        current_timestamp = datetime.now()
        
        # Prepara novos registros
        new_records = (
            current_gmv_df
            .withColumn(
                "sk_gmv",
                F.concat_ws(
                    "_",
                    F.col("reference_date").cast("string"),
                    F.col("subsidiary"),
                    F.lit(snapshot_date.isoformat())
                )
            )
            .withColumn("snapshot_date", F.lit(snapshot_date))
            .withColumn("valid_from", F.lit(snapshot_date))
            .withColumn("valid_to", F.lit(None).cast(DateType()))
            .withColumn("is_current", F.lit(True))
            .withColumn("created_at", F.lit(current_timestamp))
            .withColumn("updated_at", F.lit(current_timestamp))
        )
        
        if existing_history_df is None or existing_history_df.count() == 0:
            return new_records
        
        # Identifica registros que precisam ser atualizados
        # (mesmo reference_date + subsidiary, mas valores diferentes)
        join_condition = (
            (F.col("existing.reference_date") == F.col("new.reference_date")) &
            (F.col("existing.subsidiary") == F.col("new.subsidiary")) &
            (F.col("existing.is_current") == True)
        )
        
        # Registros existentes que serão fechados
        records_to_close = (
            existing_history_df.alias("existing")
            .join(new_records.alias("new"), join_condition, "inner")
            .select("existing.*")
            .withColumn("valid_to", F.lit(snapshot_date))
            .withColumn("is_current", F.lit(False))
            .withColumn("updated_at", F.lit(current_timestamp))
        )
        
        # Registros existentes que permanecem inalterados
        unchanged_records = (
            existing_history_df.alias("existing")
            .join(new_records.alias("new"), join_condition, "left_anti")
        )
        
        # União de todos os registros
        final_history = (
            unchanged_records
            .unionByName(records_to_close)
            .unionByName(new_records)
        )
        
        return final_history

    def run_etl(
        self, 
        purchase_path: str,
        product_item_path: str,
        extra_info_path: str,
        output_path: str,
        existing_history_path: Optional[str] = None
    ):
        """
        Executa o pipeline ETL completo.
        
        Args:
            purchase_path: Caminho dos eventos de purchase
            product_item_path: Caminho dos eventos de product_item
            extra_info_path: Caminho dos eventos de purchase_extra_info
            output_path: Caminho de saída para tabela de GMV
            existing_history_path: Caminho do histórico existente (opcional)
        """
        # 1. Leitura das fontes
        purchase_df = self.read_purchase_events(purchase_path)
        product_item_df = self.read_product_item_events(product_item_path)
        extra_info_df = self.read_purchase_extra_info_events(extra_info_path)
        
        # 2. Consolidação e tratamento de qualidade
        consolidated_df = self.process_source_tables(
            purchase_df, product_item_df, extra_info_df
        )
        
        # 3. Cálculo do GMV
        gmv_df = self.calculate_gmv(consolidated_df)
        
        # 4. Leitura do histórico existente (se houver)
        existing_history_df = None
        if existing_history_path:
            try:
                existing_history_df = self.spark.read.parquet(existing_history_path)
            except Exception:
                existing_history_df = None
        
        # 5. Aplicação do SCD Type 2
        # Snapshot date = D-1 (ontem)
        from datetime import timedelta
        snapshot_date = self.processing_date - timedelta(days=1)
        
        final_df = self.apply_scd_type2(gmv_df, existing_history_df, snapshot_date)
        
        # 6. Escrita particionada por snapshot_date
        (
            final_df
            .write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(output_path)
        )
        
        print(f"ETL concluído com sucesso! Dados salvos em: {output_path}")
        return final_df


# ============================================================================
# SIMULAÇÃO COM DADOS DE EXEMPLO (conforme PDF do desafio)
# ============================================================================

def create_sample_data(spark: SparkSession):
    """
    Cria dados de exemplo baseados no desafio técnico.
    
    Simula os eventos CDC conforme especificado no exercício.
    """
    
    # Dados de purchase (eventos/CDC) - conforme PDF
    purchase_data = [
        ("2023-01-20 22:00:00", "2023-01-20", 55, 15947, 5, "2023-01-20", "2023-01-20", 852852),
        ("2023-01-26 00:01:00", "2023-01-26", 56, 369798, 746520, "2023-01-25", None, 963963),
        ("2023-02-05 10:00:00", "2023-02-05", 69, 160001, 5, "2023-01-20", "2023-01-20", 852852),
        ("2023-02-26 03:00:00", "2023-02-26", 69, 160001, 18, "2023-02-26", "2023-02-28", 96967),
        ("2023-07-15 09:00:00", "2023-07-15", 55, 160001, 5, "2023-01-20", "2023-03-01", 852852),
    ]
    
    purchase_df = spark.createDataFrame(
        [(
            datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(row[1], "%Y-%m-%d").date(),
            row[2], row[3], row[4],
            datetime.strptime(row[5], "%Y-%m-%d").date(),
            datetime.strptime(row[6], "%Y-%m-%d").date() if row[6] else None,
            row[7],
            None,  # purchase_partition
            None,  # prod_item_partition
            None,  # purchase_total_value
            None,  # purchase_status
        ) for row in purchase_data],
        PURCHASE_SCHEMA
    )
    
    # Dados de product_item (eventos/CDC) - conforme PDF
    # Nota: O PDF mostra product_item como tabela de eventos separada
    # Simulando valores de purchase_value para calcular GMV
    product_item_data = [
        ("2023-01-20 21:00:00", "2023-01-20", 5, None, 101, 1, 150.00),      # prod_item_id=5
        ("2023-01-25 20:00:00", "2023-01-25", 746520, None, 102, 1, 200.00), # prod_item_id=746520
        ("2023-02-26 02:00:00", "2023-02-26", 18, None, 103, 1, 350.00),     # prod_item_id=18
    ]
    
    product_item_df = spark.createDataFrame(
        [(
            datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(row[1], "%Y-%m-%d").date(),
            row[2], row[3], row[4], row[5], row[6]
        ) for row in product_item_data],
        PRODUCT_ITEM_SCHEMA
    )
    
    # Dados de purchase_extra_info (eventos/CDC) - conforme PDF
    extra_info_data = [
        ("2023-01-23 00:05:00", "2023-01-23", 55, None, "nacional"),
        ("2023-01-25 23:59:59", "2023-01-25", 56, None, "internacional"),
        ("2023-02-28 01:10:00", "2023-02-28", 69, None, "nacional"),
        ("2023-03-12 07:00:00", "2023-03-12", 69, None, "internacional"),
    ]
    
    extra_info_df = spark.createDataFrame(
        [(
            datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S"),
            datetime.strptime(row[1], "%Y-%m-%d").date(),
            row[2], row[3], row[4]
        ) for row in extra_info_data],
        PURCHASE_EXTRA_INFO_SCHEMA
    )
    
    return purchase_df, product_item_df, extra_info_df


def run_example():
    """Executa exemplo do ETL com dados simulados."""
    spark = create_spark_session()
    
    # Cria dados de exemplo
    purchase_df, product_item_df, extra_info_df = create_sample_data(spark)
    
    print("=== Dados de Entrada: purchase ===")
    purchase_df.show(truncate=False)
    
    print("=== Dados de Entrada: product_item ===")
    product_item_df.show(truncate=False)
    
    print("=== Dados de Entrada: purchase_extra_info ===")
    extra_info_df.show(truncate=False)
    
    # Instancia o processador
    processor = GMVProcessor(spark)
    processor.set_processing_date(date(2023, 7, 16))
    
    # Processa as tabelas
    consolidated = processor.process_source_tables(
        purchase_df, product_item_df, extra_info_df
    )
    
    print("=== Dados Consolidados ===")
    consolidated.show(truncate=False)
    
    # Calcula GMV
    gmv = processor.calculate_gmv(consolidated)
    
    print("=== GMV Diário por Subsidiária ===")
    gmv.show(truncate=False)
    
    # Aplica SCD Type 2
    from datetime import timedelta
    snapshot_date = processor.processing_date - timedelta(days=1)
    final = processor.apply_scd_type2(gmv, None, snapshot_date)
    
    print("=== Tabela Final (GMV Histórico) ===")
    final.show(truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    run_example()
