from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World!")

with DAG(
    dag_id='hello_world',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['demo'],
) as dag:
    
    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=hello_world,
    )

