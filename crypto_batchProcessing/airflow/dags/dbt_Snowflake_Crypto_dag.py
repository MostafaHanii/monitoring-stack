
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator # keep this if you want python tasks
from airflow.operators.bash import BashOperator

def print_hello():
    print("Hello from Crypto Extraction DAG!")

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'crypto_extraction_dbt_dag',
    default_args=default_args,
    description='Extract assets, OHLCV data, then run dbt models',
    schedule='0 2 1 1 *',  # Once per year on Jan 1 at 02:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['crypto', 'extraction', 'dbt'],
) as dag:
    
    task1 = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )

    extract_assets = BashOperator(
        task_id='extract_assets',
        bash_command='python /opt/airflow/crypto_dbt/API_Scripts/assets.py'
    )

    extract_ohlcv = BashOperator(
        task_id='extract_ohlcv',
        bash_command='python /opt/airflow/crypto_dbt/API_Scripts/ohlcv_extract.py'
    )
    dbt_snap = BashOperator(
        task_id='dbt_snap',
        bash_command='cd /opt/airflow/crypto_dbt && dbt snapshot'
    )
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/crypto_dbt && dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /opt/airflow/crypto_dbt && dbt test'
    )

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/crypto_dbt && dbt debug'
    )
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='cd /opt/airflow/crypto_dbt && dbt deps'
    )
    dbt_docs = BashOperator(
        task_id='dbt_docs',
        bash_command='cd /opt/airflow/crypto_dbt && dbt docs generate'
    )
    

    task1 >> extract_assets >> extract_ohlcv >> dbt_deps >> dbt_debug >> dbt_snap >> dbt_run >> dbt_test >> dbt_docs