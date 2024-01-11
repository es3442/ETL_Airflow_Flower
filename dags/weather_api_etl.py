from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import storage
from airflow.models import Variable
from airflow.decorators import task
import requests
import pandas as pd
import csv


def fetch_weather_data():
    api_url = 'http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList'
    params = {
        'serviceKey': Variable.get('weather_serviceKey'),
        'pageNo': '1',
        'numOfRows': '365',
        'dataType': 'JSON',
        'dataCd': 'ASOS',
        'dateCd': 'DAY',
        'startDt': '20240101',
        'endDt': '20240110',
        'stnIds': '108'
    }
    response = requests.get(api_url, params=params)
    weather_data = response.json()['response']['body']['items']['item']

    # weather_data를 DataFrame으로 변환
    df = pd.DataFrame(weather_data)
    csv_string = df.to_csv(index = False)
    
    # DataFrame을 csv 파일로 저장
    # df.to_csv('weather_data_2024.csv', index=False)
    # GCSHook을 사용하여 GCS에 파일 업로드-------------------------------
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn') # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket' # 버킷이름
    object_name = 'weather_data_test.csv' # 버킷에 저장위치 및 파일명
    gcs_hook.upload(bucket_name, object_name, data=csv_string, mime_type='text/csv') # 업로드

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}    
    
with DAG(
    'weather_fetch_and_upload_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    fetch_weather_data_task = PythonOperator(
        task_id='weather_fetch_and_upload',
        python_callable=fetch_weather_data,
    )