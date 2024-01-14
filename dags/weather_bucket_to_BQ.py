from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'schedule': "@once",
    'catchup': False,
    'tags': ["gcs"],
}

with DAG(
    'weather_gcs_to_bigquery_dag',
    default_args=default_args,
) as dag:

    weather_load_csv_delimiter = GCSToBigQueryOperator(
        task_id="weather_load_to_bigquery",
        bucket='flower-pipeline-bucket',
        source_objects=["weather/weather_data.csv"],
        source_format="csv",
        destination_project_dataset_table='weather_bq.weather_test_table',
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        gcp_conn_id="gcp_conn", 
        deferrable=True,
    )
    
    weather_load_csv_delimiter

