#imports

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import json
import boto3


def get_data_from_api():
    # Função para obter dados da API
    response = requests.get("URL_DA_API")
    data = response.json()
    return data


def upload_to_s3():
    # Função para fazer o upload dos dados para o bucket S3
    data = get_data_from_api()
    # Desnecessário se a conexão já existe
    # s3 = boto3.client("s3", aws_access_key_id="ACCESS_KEY_ID", aws_secret_access_key="SECRET_ACCESS_KEY")
    bucket_name = "NOME_DO_BUCKET"
    file_name = "nome_do_arquivo.json"
    s3.put_object(
        Body=json.dumps(data),
        Bucket=bucket_name,
        Key=file_name
    )


# Definindo a DAG
dag = DAG(
    "api_to_s3_dag",
    description="DAG para consumir dados de uma API e enviar para o Amazon S3",
    schedule_interval=None,
    start_date=datetime(2023, 7, 3),
    catchup=False
)

# Definindo as tarefas da DAG
# Aqui depende de onde vai salvar os dados. 
get_data_task = PythonOperator(
    task_id="get_data_from_api",
    python_callable=get_data_from_api,
    dag=dag
)

upload_task = PythonOperator(
    task_id="upload_to_s3",
    python_callable=upload_to_s3,
    dag=dag
)

# Definindo a ordem das tarefas
get_data_task >> upload_task
