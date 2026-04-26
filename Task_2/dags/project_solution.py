from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 4, 26),
}

with DAG(
    'multi_hop_datalake_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Pipeline for processing athlete data from Landing to Gold'
) as dag:

    landing_to_bronze = SparkSubmitOperator(
    task_id='landing_to_bronze',
    application='/opt/airflow/dags/scripts/landing_to_bronze.py',
    conn_id='spark_default',
    verbose=True
    )

    bronze_to_silver = SparkSubmitOperator(
    task_id='bronze_to_silver',
    application='/opt/airflow/dags/scripts/bronze_to_silver.py',
    conn_id='spark_default',
    verbose=True
    )

    silver_to_gold = SparkSubmitOperator(
    task_id='silver_to_gold',
    application='/opt/airflow/dags/scripts/silver_to_gold.py',
    conn_id='spark_default',
    verbose=True
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold