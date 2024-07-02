
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

class TransformRaw:

    def __init__(self, **kwargs) -> None:

        # STORAGE ARCHITECTURE
        self.path_raw: str          = kwargs.get('path_raw')
        self.path_transformed: str  = kwargs.get('path_transformed')

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
                StructField('latitude', StringType(), True),
                StructField('longitude', StringType(), True),
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
            self.spark.read.format('json') # TIPO DO ARQUIVO
            .schema(self.schema) # CONFIGURANDO SCHEMA PARA LEITURA DO ARQUIVO
            .option("multiLine", True) # ARQUIVO JSON TIPO MULTILINE
            .load(self.path_raw) # PATH DO ARQUIVO JSON
            .withColumn('latitude', col('latitude').cast(DoubleType())) # CAST DE STRING PARA DOUBLE
            .withColumn('longitude', col('longitude').cast(DoubleType())) # CAST DE STRING PARA DOUBLE
        )

        df.write.parquet(self.path_transformed, mode='overwrite', partitionBy='state')

        self.spark.stop()

if __name__ == "__main__":

    path_raw = "bronze_layer/breweries.json"
    path_transformed = "silver_layer/breweries"

    # path_raw = sys.argv[1]
    # path_transformed = sys.argv[2]

    TransformRaw(
        path_raw=path_raw,
        path_transformed=path_transformed
    ).transformData()