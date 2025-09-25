from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    process_taxi_data = SparkSubmitOperator(
        task_id="process_taxi_data",
        application="/opt/airflow/dags/spark_jobs/taxi_etl.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--input", "/opt/airflow/data/january",
            "--output", "mongodb://host.docker.internal:27017/nyc_taxi"
        ],
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        executor_memory="2g",
        driver_memory="1g",
    )