from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def hello_world():
    print("Hello World from Airflow!")


with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger
    catchup=False,
    tags=["test", "hello"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_world_task",
        python_callable=hello_world,
    )

    hello_task
