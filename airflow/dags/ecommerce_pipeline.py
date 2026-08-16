from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def bronze():
    print("Running Bronze layer")


def silver():
    print("Running Silver layer")


def data_quality():
    print("Running Data Quality checks")


def gold():
    print("Running Gold layer")


with DAG(
    dag_id="ecommerce_data_pipeline",
    start_date=datetime(2026, 8, 13),
    schedule=None,
    catchup=False,
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze",
        python_callable=bronze,
    )

    silver_task = PythonOperator(
        task_id="silver",
        python_callable=silver,
    )

    quality_task = PythonOperator(
        task_id="data_quality",
        python_callable=data_quality,
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=gold,
    )

    bronze_task >> silver_task >> quality_task >> gold_task
