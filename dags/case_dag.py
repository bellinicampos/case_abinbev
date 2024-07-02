import pendulum
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from classe_case import CaseABInBev
from slack_notification import notification

classe = CaseABInBev()

default_args = {
    'owner': 'Pedro Bellini',
    'depends_on_past': False,
    'email_on_failure': False, # PODE SER UMA FORMA DE ALERTA
    'email_on_retry': False,
    'on_failure_callback': notification, # EN CASO DE FALHA, ENVIA MSG EM CANAL DO SLACK
    'retries': 3, # TENTA 3 VEZES ANTES DE FALHAR
    'retry_delay': timedelta(minutes=5) # INTERVALO ENTRE AS TENTATIVAS
}

with DAG(
    dag_id= "case_abinbev",
    default_args= default_args,
    start_date= pendulum.datetime(2024,6,29),
    schedule_interval= '0 7 * * *',
    max_active_runs= 1,
    catchup= False,
    description= "Desenvolvida para o case da AB InBev",
    tags= ['Case', 'AB InBev']
) as dag:
    
    # EXTRAI DADOS DA API EM SALVA NA BRONZE LAYER
    api_extraction = PythonOperator(
        task_id= "api_extraction",
        python_callable= classe.api_extraction,
        provide_context= True
    )

    # TRANSFORMA JSON EM PARQUET PARTICIONADO POR LOCALIZACAO
    pyspark_transform_to_silver = DataprocCreateBatchOperator(
        task_id= "pyspark_transform_to_silver",
        batch= classe.pyspark_batch_config(
            classe.pyspark_transform_to_silver,
            [
                classe.path_raw_json,
                classe.path_transformed
            ]
        ),
        batch_id= classe.batch_id_transform,
        region= classe.region,
        gcp_conn_id= classe.gcp_conn_id
    )

    # AGREGA DADOS POR TIPO E LOCALIZACAO E SALVA NA GOLD LAYER
    pyspark_transform_to_gold = DataprocCreateBatchOperator(
        task_id= "pyspark_transform_to_gold",
        batch= classe.pyspark_batch_config(
            classe.pyspark_transform_to_gold,
            [
                classe.path_transformed,
                classe.path_aggregated_view
            ]
        ),
        batch_id= classe.batch_id_aggregate,
        region= classe.region,
        gcp_conn_id= classe.gcp_conn_id
    )

    # EXECUTION ORDER
    api_extraction >> pyspark_transform_to_silver >> pyspark_transform_to_gold