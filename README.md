## INSTRUÇÕES

Configure a conexão com o GCP se for rodar o job no Dataproc.
- Cria uma service account com as permissões necessárias e baixe o JSON
- Na interface do Airflow, em Admin > Connections, crie uma conexão apontando o keyfile path para o JSON

Também é possível configurar uma conexão para rodar o job no próprio Airflow (para teste do case)
- spark_default = {"conn_type": "spark", "host": "spark://your_spark_master_host:7077"}

Também é possível criar na GCP um serviço do Cloud Composer, que é o serviço de Airflow gerenciado da GCP.

O Cloud Composer tem as conexões da GCP configuradas por padrão, podendo simplesmente usar o operador do Dataproc Batch para executar o job.

A solução foi dividida em 4 arquivos, a dag, a classe com as variáveis e métodos usados na dag e dois jobs pyspark, o primeiro leva os dados para a Silver e o segundo para a Gold.