from asyncio import Task
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.hooks.cassandra import CassandraHook
from cassandra.cluster import Cluster
import json
import os

cassandra_hook = CassandraHook("cassandra_default")

default_args = {
    'owner': 'mateuszbar',
    'depends_on_past': False,
    'email': ['matibaran3141@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=2),
    'end_date': datetime(2024, 1, 1)
}

def get_json_data(**context):
    task_instance = context["ti"]
    path_to_json = '/home/mateusz/Coding/Coding/temp_pinterestdata/'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    file_path = path_to_json + json_files[0]
    json_data = []
    for line in open(file_path, 'r'):
        json_data.append(json.loads(line))
    task_instance.xcom_push("json_data", json_data)

def insert_into_cassandra_db(**context):
    task_instance = context["ti"]
    pinterest_data = task_instance.xcom_pull(task_ids='get_json_data', 
                                            key='json_data'
                                            )
    cluster = Cluster(['127.0.0.1'], port = 9042)
    session = cluster.connect

    for i in range(0, len(pinterest_data)):
        session.execute(
        """
        INSERT INTinterest_data (category, description, downloaded, follower_count, image_src, idx, is_image_or_video, save_location, tag_list, title, unique_id)
        VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s,  %s )
        """,(pinterest_data[i]["category"], pinterest_data[i]["description"], 
        pinterest_data[i]["downloaded"], pinterest_data[i]["follower_count"], 
        pinterest_data[i]["image_src"], pinterest_data[i]["idx"],
        pinterest_data[i]["is_image_or_video"], pinterest_data[i]["save_location"], 
        pinterest_data[i]["tag_list"], pinterest_data[i]["title"], pinterest_data[i]["unique_id"])
        )
        print("Done Sending data")

with DAG(dag_id='daily_spark_job',
         default_args=default_args,
         schedule_interval='@hourly',
         catchup=False,
         tags=['pinterest','pipeline', 'spark']
         ) as dag:
    
    run_spark_job = BashOperator(
        task_id='spark_job',
        bash_command='cd /home/mateusz/apache-cassandra-3.11.13 && python3 /home/mateusz/Coding/Coding/API/spark_batch.py')
    run_get_json_data = PythonOperator(
        task_id='get_json_data',
        python_callable=get_json_data)
    run_insert_into_cassandra_db = PythonOperator(
        task_id='insert_into_cassandra_db',
        python_callable=insert_into_cassandra_db)
            
    run_spark_job >> run_get_json_data >> run_insert_into_cassandra_db