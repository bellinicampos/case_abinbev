import requests
import json
from datetime import datetime

class CaseABInBev:

    """
    Classe criada para uso no pipeline de extração e tratamento dos dados de cervejaria
    """

    def __init__(self):

        # GENERAL VARIABLES
        self.current_date: str          = datetime.now().strftime("%Y%m%d") # DATA NO FORMATO YYYYMMDD

        # GOOGLE CLOUD ENV CONFIGS
        self.project_id: str            = "case_abinbev" # COLOQUE O NOME DO SEU PROJETO AQUI
        self.region: str                = "us-central1" # COLOQUE A REGIAO QUE ESTA USANDO AQUI
        self.gcp_conn_id: str           = "google_cloud_default" # CONEXAO PADRAO DO AIRFLOW PARA O GCP

        # STORAGE ARCHITECTURE
        self.bronze_layer: str          = "bronze_layer" # Na GCP, o path seria gs://NOME_DO_BUCKET
        self.silver_layer: str          = "silver_layer"
        self.gold_layer: str            = "gold_layer"

        # API EXTRACTION VARIABLES
        self.api_uri: str               = "https://api.openbrewerydb.org/breweries"
        self.path_raw_json: str         = f"{self.bronze_layer}/breweries/{self.current_date}.json"
        self.path_transformed: str      = f"{self.silver_layer}/breweries/{self.current_date}"
        self.path_aggregated_view: str  = f"{self.gold_layer}/aggregated_view/{self.current_date}"

        # PYSPARK BATCH CONFIG
        self.batch_id_transform: str            = f"transform_breweries_{self.current_date}"
        self.batch_id_aggregate: str            = f"aggregate_breweries_{self.current_date}"
        self.pyspark_transform_to_silver: str   = "spark_jobs/pyspark_transform_to_silver.py"
        self.pyspark_transform_to_gold: str     = "spark_jobs/pyspark_transform_to_gold.py"
    
    def api_extraction(self) -> None:

        """
        Faz a requisição get na API e se obtiver sucesso, salva na Bronze layer no formato JSON
        """

        response = requests.get(self.api_uri)

        if response.status_code == 200:

            with open(self.path_raw_json, 'w') as json_file:

                json.dump(
                    response.json(),
                    json_file,
                    indent= 4
                )

        else:
            raise ValueError(f"Request failed with status {response.status_code}")

    def pyspark_batch_config(self, main_python_file_uri: str, args: list) -> dict:

        """
        Configurações do job pyspark passada no operador da dag
        Recebe o path do job e uma lista com os argumentos
        """

        return {
            "pyspark_batch": {
                "main_python_file_uri": main_python_file_uri,
                "args": args
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": f"projects/{self.project_id}/regions/{self.region}/subnetworks/default"
                }
            }
        }