
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class TransformRaw:

    def __init__(self, **kwargs) -> None:

        # STORAGE ARCHITECTURE
        self.path_silver: str       = kwargs.get('path_silver')
        self.path_gold: str         = kwargs.get('path_gold')

        # SCHEMA DO JSON
        self.schema: StructType     = StructType(
            [
                StructField('id', StringType(), True),
                StructField('name', StringType(), True),
                StructField('brewery_type', StringType(), True),
                StructField('phone', StringType(), True),
                StructField('address_1', StringType(), True),
                StructField('address_2', StringType(), True),
                StructField('address_3', StringType(), True),
                StructField('city', StringType(), True),
                StructField('latitude', DoubleType(), True),
                StructField('longitude', DoubleType(), True),
                StructField('website_url', StringType(), True),
                StructField('street', StringType(), True),
                StructField('postal_code', StringType(), True),
                StructField('state_province', StringType(), True),
                StructField('state', StringType(), True),
                StructField('country', StringType(), True)
            ]
        )

        # CONFIGURACOES DA SESSAO DO SPARK
        self.spark: SparkSession    = SparkSession.builder.appName("TransformBreweriesRaw").getOrCreate()
        self.spark.conf.set('spark.sql.session.timeZone', 'America/Sao_Paulo')        
        self.spark.conf.set('spark.sql.caseSensitive', True)        
        self.spark.sparkContext.setLogLevel("WARN")
        
    def transformData(self) -> None:

        df = (
            self.spark.read.format('parquet') # TIPO DO ARQUIVO
            .schema(self.schema) # CONFIGURANDO SCHEMA PARA LEITURA DO ARQUIVO
            .load(self.path_silver) # PATH DOS ARQUIVOS PARQUET
        )

        agg_df = (
            df.groupBy("brewery_type", "state")
            .agg(count("*").alias("brewery_count"))
        )

        agg_df.show(truncate=False)

        agg_df.write.parquet(self.path_gold, mode='overwrite')

        self.spark.stop()

if __name__ == "__main__":

    # path_silver = "silver_layer/breweries"
    # path_gold = "gold_layer/aggregated_view"

    path_silver = sys.argv[1]
    path_gold = sys.argv[2]

    TransformRaw(
        path_silver= path_silver,
        path_gold= path_gold
    ).transformData()