from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

import os
from os import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from app.pipelines import bronze, silver, gold

with DAG(
    dag_id="breweries-ingestion",
    schedule=None,
    start_date=datetime(year=2024, month=7, day=31),
    end_date=None,
    catchup=False,
    tags=["breweries", "api", "ingestion"],
) as breweries_dag:

    bronze_operator = PythonOperator(
        dag=breweries_dag,
        task_id="bronze_pipeline",
        python_callable=bronze.main,
        op_kwargs={"pages": list(range(1, 21)), "per_page": 20, "extract_all": False},
    )

    silver_operator = PythonOperator(
        dag=breweries_dag,
        task_id="silver_pipeline",
        python_callable=silver.main,
        op_kwargs={},
    )

    gold_operator = PythonOperator(
        dag=breweries_dag,
        task_id="gold_pipeline",
        python_callable=gold.main,
        op_kwargs={},
    )

    bronze_operator >> silver_operator >> gold_operator
