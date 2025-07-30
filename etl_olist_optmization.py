import logging
import sys
import os
import requests
import zipfile
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, monotonically_increasing_id, to_date, year, month, dayofmonth, 
    quarter, dayofweek, date_format, udf, avg, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, FloatType
)
from typing import Dict

class OptimizedStarSchemaPipeline:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.spark = self._initialize_spark()
        self.source_tables: Dict[str, DataFrame] = {}
        self.dimensional_models: Dict[str, DataFrame] = {}
        self._setup_logging()

    def _load_config(self, config_path: str) -> Dict:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        logging.basicConfig(level=self.config["log_level"], format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", stream=sys.stdout)
        self.logger = logging.getLogger(self.config["pipeline_name"])
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

    def _initialize_spark(self) -> SparkSession:
        try:
            # Aumenta o limite para auto-broadcast para 100MB
            # Em um cluster real, este valor deve ser ajustado com cuidado.
            auto_broadcast_threshold = self.config["spark"].get("auto_broadcast_threshold", "10485760") # Default 10MB
            return (
                SparkSession.builder
                .appName(self.config["spark"]["app_name"])
                .master(self.config["spark"]["master"])
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.sql.autoBroadcastJoinThreshold", auto_broadcast_threshold)
                .getOrCreate()
            )
        except Exception as e:
            logging.critical(f"Falha ao inicializar SparkSession: {e}", exc_info=True)
            sys.exit(1)
    
    def _download_and_unzip_data(self):
        source_path = self.config["data"]["source_path"]
        if os.path.exists(source_path) and any(f.endswith('.csv') for f in os.listdir(source_path)):
            self.logger.info("Arquivos de dados já existem. Pulando o download.")
            return

        os.makedirs(source_path, exist_ok=True)
        zip_path = os.path.join(source_path, "olist_dataset.zip")
        try:
            self.logger.info(f"Baixando dados de {self.config['data']['url']}...")
            response = requests.get(self.config["data"]["url"], stream=True, timeout=60)
            response.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192): f.write(chunk)
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref: zip_ref.extractall(source_path)
            os.remove(zip_path)
        except Exception as e:
            self.logger.critical(f"Falha no download ou descompactação dos dados: {e}", exc_info=True)
            sys.exit(1)

    def _load_source_tables(self, schemas: Dict[str, StructType]):
        source_path = self.config["data"]["source_path"]
        for name, schema in schemas.items():
            file_path = f"{source_path}/olist_{name.replace('_', '_dataset_')}.csv"
            self.source_tables[name] = self.spark.read.csv(file_path, header=True, schema=schema)

    def _create_dimensions(self):
        self.logger.info("Iniciando a criação e cache das tabelas de dimensão...")
        
        # Cada dimensão é criada e imediatamente cacheada para reutilização.
        # A ação .count() força a materialização do cache.
        
        dim_geo = self.source_tables["geolocation"].groupBy("geolocation_zip_code_prefix").agg(
            avg("geolocation_lat").alias("latitude"),
            avg("geolocation_lng").alias("longitude")
        ).withColumn("id_geolocalizacao", monotonically_increasing_id()).cache()
        self.logger.info(f"Dim_Geolocalizacao criada com {dim_geo.count()} registros. Cacheada.")
        self.dimensional_models["Dim_Geolocalizacao"] = dim_geo

        df_customers = self.source_tables["customers"]
        dim_cliente = df_customers.join(broadcast(dim_geo), df_customers.customer_zip_code_prefix == dim_geo.geolocation_zip_code_prefix, "left") \
            .select(col("customer_unique_id").alias("id_negocio_cliente"), col("customer_city").alias("cidade_cliente"), col("customer_state").alias("estado_cliente"), col("id_geolocalizacao")) \
            .distinct().withColumn("id_cliente", monotonically_increasing_id()).cache()
        self.logger.info(f"Dim_Cliente criada com {dim_cliente.count()} registros. Cacheada.")
        self.dimensional_models["Dim_Cliente"] = dim_cliente

        df_products = self.source_tables["products"]
        df_translation = self.source_tables["translation"]
        capitalize_udf = udf(lambda s: s.replace("_", " ").title() if s else "N/A", StringType())
        dim_produto = df_products.join(df_translation, "product_category_name", "left") \
            .select(col("product_id").alias("id_negocio_produto"), capitalize_udf(col("product_category_name_english")).alias("categoria_produto")) \
            .distinct().withColumn("id_produto", monotonically_increasing_id()).cache()
        self.logger.info(f"Dim_Produto criada com {dim_produto.count()} registros. Cacheada.")
        self.dimensional_models["Dim_Produto"] = dim_produto

        df_sellers = self.source_tables["sellers"]
        dim_vendedor = df_sellers.join(broadcast(dim_geo), df_sellers.seller_zip_code_prefix == dim_geo.geolocation_zip_code_prefix, "left") \
            .select(col("seller_id").alias("id_negocio_vendedor"), col("seller_city").alias("cidade_vendedor"), col("seller_state").alias("estado_vendedor"), col("id_geolocalizacao")) \
            .distinct().withColumn("id_vendedor", monotonically_increasing_id()).cache()
        self.logger.info(f"Dim_Vendedor criada com {dim_vendedor.count()} registros. Cacheada.")
        self.dimensional_models["Dim_Vendedor"] = dim_vendedor

        df_orders = self.source_tables["orders"]
        dim_tempo = df_orders.select(to_date(col("order_purchase_timestamp")).alias("data")).distinct().na.drop() \
            .select(col("data"), year("data").alias("ano"), month("data").alias("mes"), dayofmonth("data").alias("dia"), quarter("data").alias("trimestre")) \
            .withColumn("id_tempo", monotonically_increasing_id()).cache()
        self.logger.info(f"Dim_Tempo criada com {dim_tempo.count()} registros. Cacheada.")
        self.dimensional_models["Dim_Tempo"] = dim_tempo

    def _create_fact_table(self):
        self.logger.info("Iniciando a criação da tabela Fato_Vendas com Broadcast Joins...")
        base_fato = self.source_tables["order_items"].join(self.source_tables["orders"], "order_id", "inner") \
            .join(self.source_tables["customers"].select("customer_id", "customer_unique_id"), "customer_id", "inner")
        
        # Aplicando broadcast() em todas as junções com as dimensões (pequenas)
        fato_com_chaves = base_fato \
            .join(broadcast(self.dimensional_models["Dim_Produto"]), base_fato.product_id == self.dimensional_models["Dim_Produto"].id_negocio_produto, "left") \
            .join(broadcast(self.dimensional_models["Dim_Cliente"]), base_fato.customer_unique_id == self.dimensional_models["Dim_Cliente"].id_negocio_cliente, "left") \
            .join(broadcast(self.dimensional_models["Dim_Vendedor"]), base_fato.seller_id == self.dimensional_models["Dim_Vendedor"].id_negocio_vendedor, "left") \
            .join(broadcast(self.dimensional_models["Dim_Tempo"]), to_date(base_fato.order_purchase_timestamp) == self.dimensional_models["Dim_Tempo"].data, "left")
        
        self.dimensional_models["Fato_Vendas"] = fato_com_chaves.select(
            col("id_produto"), col("id_cliente"), col("id_vendedor"), col("id_tempo"),
            col("order_id").alias("id_pedido"), col("price").alias("preco"),
            col("freight_value").alias("valor_frete")
        ).na.drop()

    def _save_models(self):
        self.logger.info("Salvando modelos dimensionais...")
        output_path = self.config["data"]["output_path"]
        for name, df in self.dimensional_models.items():
            df.write.mode("overwrite").parquet(os.path.join(output_path, name))

    def _run_analytical_query(self):
        self.logger.info("Executando consulta analítica otimizada...")
        for name, df in self.dimensional_models.items():
            df.createOrReplaceTempView(name.lower())
        
        # Esta consulta agora lerá as dimensões do cache, sendo muito mais rápida.
        query_resultado = self.spark.sql("""
            SELECT t.ano, p.categoria_produto, SUM(f.preco + f.valor_frete) AS faturamento_total
            FROM fato_vendas f
            JOIN dim_produto p ON f.id_produto = p.id_produto
            JOIN dim_tempo t ON f.id_tempo = t.id_tempo
            WHERE t.ano = 2018 AND p.categoria_produto IS NOT NULL AND p.categoria_produto != 'N/A'
            GROUP BY t.ano, p.categoria_produto
            ORDER BY faturamento_total DESC
            LIMIT 10
        """)
        
        self.logger.info("Resultado: Top 10 Categorias por Faturamento em 2018")
        query_resultado.show()

    def run(self, schemas: Dict[str, StructType]):
        self.logger.info("--- INICIANDO PIPELINE OTIMIZADO ---")
        try:
            self._download_and_unzip_data()
            self._load_source_tables(schemas)
            self._create_dimensions()
            self._create_fact_table()
            self._save_models()
            self._run_analytical_query()
            self.logger.info("--- PIPELINE OTIMIZADO CONCLUÍDO COM SUCESSO ---")
        except Exception as e:
            self.logger.critical(f"--- FALHA NA EXECUÇÃO DO PIPELINE: {e} ---", exc_info=True)
            sys.exit(1)
        finally:
            self.spark.stop()
            self.logger.info("SparkSession finalizada.")

if __name__ == "__main__":
    CONFIG_FILE = "config.yaml"
    SCHEMAS = {
        "customers": StructType([StructField("customer_id", StringType()), StructField("customer_unique_id", StringType()), StructField("customer_zip_code_prefix", IntegerType()), StructField("customer_city", StringType()), StructField("customer_state", StringType())]),
        "sellers": StructType([StructField("seller_id", StringType()), StructField("seller_zip_code_prefix", IntegerType()), StructField("seller_city", StringType()), StructField("seller_state", StringType())]),
        "products": StructType([StructField("product_id", StringType()), StructField("product_category_name", StringType()), StructField("product_name_lenght", IntegerType()), StructField("product_description_lenght", IntegerType()), StructField("product_photos_qty", IntegerType())]),
        "orders": StructType([StructField("order_id", StringType()), StructField("customer_id", StringType()), StructField("order_status", StringType()), StructField("order_purchase_timestamp", TimestampType())]),
        "order_items": StructType([StructField("order_id", StringType()), StructField("order_item_id", IntegerType()), StructField("product_id", StringType()), StructField("seller_id", StringType()), StructField("price", DoubleType()), StructField("freight_value", DoubleType())]),
        "translation": StructType([StructField("product_category_name", StringType()), StructField("product_category_name_english", StringType())]),
        "geolocation": StructType([StructField("geolocation_zip_code_prefix", IntegerType()), StructField("geolocation_lat", FloatType()), StructField("geolocation_lng", FloatType()), StructField("geolocation_city", StringType()), StructField("geolocation_state", StringType())])
    }

    # Assumindo que config.yaml existe no diretório
    pipeline = OptimizedStarSchemaPipeline(config_path=CONFIG_FILE)
    pipeline.run(schemas=SCHEMAS)
