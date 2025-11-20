from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_etl_bulk_sequential",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:
    process_bulk = SparkSubmitOperator(
        task_id="process_taxi_data_bulk",
        application="/opt/airflow/dags/spark_jobs/taxi_bulk_etl.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--input", "/opt/airflow/data/",
            "--output", "mongodb://host.docker.internal:27017/nyc_taxi.trips_bulk_2025"
        ],
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.executor.instances": "6",     # == total-executor-cores / executor.cores
            "spark.executor.cores": "1",         # == CLI spark.executor.cores=1
            "spark.executor.memory": "1g",       # == CLI spark.executor.memory=1g
            "spark.cores.max": "6",
        },
    )
