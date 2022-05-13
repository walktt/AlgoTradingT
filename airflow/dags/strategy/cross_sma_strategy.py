from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from utils import strategy
from datetime import timedelta
import os

DAG_ID = os.path.basename(__file__).replace('.pyc', '').replace('.py', '')
CONN_ID = 'postgres_stocks'

SMA_SHORT=50
SMA_LONG=100

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval='10 1 * * *',
) as dag:

    cross_sma_rih = PythonOperator(
        task_id='cross_sma_rih',
        python_callable=strategy.apply_strategy,
        op_kwargs={
            'connector':CONN_ID,
            'source_table_name':'rih',
            'ticker':'RIM2',
            'strategy_func': strategy.cross_sma_strategy,
            'op_kwargs': {
                'sma_short': SMA_SHORT,
                'sma_long': SMA_LONG
            }
        }
    )
