from datetime import datetime
from os.path import join
from pathlib import Path
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.utils.dates import days_ago

# aula 6: criar um dicionaro de argumentos padrao do dag

ARGS = {
    "owner": "airflow", # owner pode ser o developer ou o time
    "depends_on_past":False, # para cada instancia do dag, precisa saber se ele precisa de uma isntancia anterior
    # nesse caso, as datas nao sao dependentes
    "start_date": days_ago(6)
}

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z" # formato de hora que o tt precisa

# aula 6: tornar caminhos mais genericos

BASE_FOLDER = join(
    str(Path("~/home/mvrm").expanduser()),
    "alura/datapipeline/datalake/{stage}/twitter_aluraonline/{partition}"
)

# aula 6

PARTITION_FOLDER = "extract_date={{ ds}}"

with DAG(
dag_id="twitter_dag", 
default_args=ARGS, 
schedule_interval="0 9 * * *",
max_active_runs=1 # paralelismo
) as dag:
    # schedule_interval="0 9 * * *" formato cron: minutos horas dia mes semana | podia tbm ser @daily
    twitter_operator = TwitterOperator(
        task_id = "twitter_aluraonline",
        query = "AluraOnline",
        file_path = join(
                
                # "/home/mvrm/alura-projetos/datapipeline/datalake", antes da aula 6
                #"twitter_aluraonline", # cada pasta, uma tabela
                BASE_FOLDER.format(stage="bronze", partition=PARTITION_FOLDER),
                
                #"extract_date={{ ds }}",
                # tabela 
                # particoes | no banco de dados nao temos indices para facilitar pesquisas, mas temos particicoes, q num datalake podem ser usado para filtrar pesquisa 
                "AluraOnline_{{ ds_nodash }}.json" # toda vez que executar, quer que reescreva o arquivo com o que esta criando
            # ds_nodash -> ds retorna uma string com a data de execução da tarefa
            # no dash vai tirar pontos e traços e barras 
            ),
            # pegar apenas do dia anterior para evitar de duplicar dados
            # execution date: data e hora do momento de execucao
            # colocar um operador que vai chamar o spark summit operator, mas dentro do airflow
            start_time = (
            "{{"
            f"execution_date.strftime('{TIMESTAMP_FORMAT}')" 
            "}}"
            ),
            end_time = (
            "{{"
            f"next_execution_date.strftime('{TIMESTAMP_FORMAT}')" 
            "}}"
            )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        # application recebe o caminho do arquivo  que vamos executar
        application = join(
            # "/home/mvrm/alura-projetos/datapipeline/spark/transformation.py" antes da aula 6
            str(Path(__file__).parents[2]),
            "spark/transformation.py"
            ),
        # nome que o spark vai dar
        name = "twitter_transformation",
        # 3 argumentos do args que tem no spark/transformation.py
        application_args = [

            "--src",
            # "/home/mvrm/alura-projetos/datapipeline/datalake/bronze/twitter_aluraonline/extract_date=2021-12-28",
            BASE_FOLDER.format(stage="bronze",partition=PARTITION_FOLDER),
            "--dest",
            # "/home/mvrm/alura-projetos/datapipeline/datalake/silver/twitter_aluraonline",
            BASE_FOLDER.format(stage="silver", partition=""),
            "--process-date",
            "{{ ds }}"
        ]

        # pro spark submit funcionar, precisamos configurar a conexao que ele vai usar dentro do airflow
        # spark usa, por padrao, o spark_default, poderia alterar o conn_id
        # mas alteramos o spark_default por ser mais facil
        # host: local
        # extra: spark-home: /home/mvrm/spark-3.2.0-bin-hadoop3.2

    )

    """
    pra testar: aula 5
    
    parar o airflow webserver

    1- airflow dags list -> listar os dags que tem na pasta dags
    2- airflow tasks list twitter_dag -> listar as tarefas da dag
    3- airflow tasks test twitter_dag transform_twitter_aluraonline 2021-12-22 -> testar uma task
    """

    # aula 6: conectar no dag -> twitter alura online e twitter operator
    twitter_operator >> twitter_transform #  >> conecta tarefas para elas serem executadas em sequencia