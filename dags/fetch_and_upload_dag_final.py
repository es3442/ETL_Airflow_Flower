from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import pandas as pd
import requests
from io import StringIO
from airflow.models import Variable
import csv

# API Data -> GCS bucket
def fetch_and_upload(**kwargs):
    # 현재 DAG 실행일을 가져옴
    execution_date = kwargs['execution_date']
    base_date_str = execution_date.strftime('%Y-%m-%d')

    # API 요청 및 응답 처리 : 각자 API에 맞게 수정------------------------
    api_url = "https://flower.at.or.kr/api/returnData.api"
    common_params = {
        'kind': 'f001',
        'serviceKey': Variable.get('flower_serviceKey')
        'baseDate': base_date_str,
        'flowerGubn': '1',
        'dataType': 'json',
        'countPerPage': '3000'
    }
    flower_gubn_values = [1, 2, 3, 4]
    all_items=[]
    for flower_gubn in flower_gubn_values:
        count_per_page = 3000
        current_page = 1
        # flowerGubn 파라미터를 업데이트
        params = common_params.copy()  # 공통 파라미터를 복사
        params['flowerGubn'] = str(flower_gubn)  # 현재 flower_gubn 값을 추가

        try:
            while True:
                params['currentPage']=str(current_page)
                response = requests.get(api_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    items = data['response']['items']
                    all_items.extend(items)
                    print(len(items))
                    print(len(all_items))
                    if len(items) < count_per_page:
                        break

                    current_page += 1
                else:
                    print(f"Failed to fetch data. Status code: {response.status_code}")
                    raise Exception("API request failed.")
        except Exception as e:
            # 실패 시에 예외가 발생하면 DAG를 다음 날로 연기
            raise Exception("DAG execution failed. Retry on the next day.")

    if len(all_items) == 0:
        print("No data to upload to GCS.")
    else:
        csv_data = StringIO()
        csv_writer = csv.DictWriter(csv_data, fieldnames=all_items[0].keys())
        csv_writer.writeheader()
        csv_writer.writerows(all_items)

        # CSV 데이터를 읽고 utf-8로 인코딩하여 바이트로 변환
        csv_data_bytes = csv_data.getvalue().encode('utf-8')

        # GCSHook을 사용하여 GCS에 파일 업로드-------------------------------
        gcs_hook = GCSHook(gcp_conn_id='gcp_conn')  # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
        bucket_name = 'flower-pipeline-bucket'  # 버킷이름
        object_name = f'flower/{common_params["baseDate"]}.csv'  # 버킷에 저장위치 및 파일명
        gcs_hook.upload(bucket_name, object_name, data=csv_data_bytes, mime_type='text/csv')  # 업로드
        # ------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'catchup': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=65),
}

with DAG(
    'fetch_and_upload_dag_final',
    default_args=default_args,
    #schedule_interval='10 13 * * *',
    schedule_interval='0 14 * * *',  # UTC 기준 14:00 (한국 시간 기준 23:00)
) as dag:

    fetch_and_upload_task = PythonOperator(
        task_id='fetch_and_upload',
        python_callable=fetch_and_upload,
    )

