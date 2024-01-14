from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def gradulation_schedule_gcs_to_bigquery():
    
    # GCP 연결 ID(서비스 계정 키: 값은 Airflow Connection 참고)
    gcp_conn_id = 'gcp_conn'
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, use_legacy_sql=False)
    client = hook.get_client() # 빅쿼리 클라이언트 생성


    # GCP 프로젝트 ID와 BigQuery 데이터셋 ID 설정
    project_id = 'learnde-4-1'
    dataset_id = 'flower_bq'

    # BigQuery 테이블 ID 설정
    table_id = 'gradulation_schedule'
    full_table_id = f'{project_id}.{dataset_id}.{table_id}'

    # CSV 파일이 저장된 GCS 경로
    gcs_uri = 'gs://flower-pipeline-bucket/academic_calendar/gradulation_schedule.csv'

    # BigQuery 테이블 스키마 설정 (필요에 따라 수정)
    schema = [
    	bigquery.SchemaField("date", "DATE"),
    	bigquery.SchemaField("event_count", "INTEGER"),
        # 추가적인 필드들을 필요에 따라 정의
    ]

    # 테이블 삭제 (이미 존재하는 경우)
    try:
        client.get_table(full_table_id)
        client.delete_table(full_table_id)
        print(f'Table {full_table_id} deleted.')
    except NotFound:
        print(f'Table {full_table_id} does not exist.')

    # CSV 파일을 BigQuery 테이블로 적재
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # CSV 파일의 헤더를 무시
        autodetect=True,      # 스키마 자동 감지
        schema=schema,        # 테이블 스키마 설정
        max_bad_records=10,  # 허용하는 최대 오류 행 수
        encoding="UTF-8",  # CSV 파일의 인코딩을 UTF-8 지정
    )

    load_job = client.load_table_from_uri(
        gcs_uri, f'{project_id}.{dataset_id}.{table_id}', job_config=job_config
    )

    load_job.result()  # 작업 완료까지 대기

    print(f'Loaded {load_job.output_rows} rows into {table_id}')

default_args = {
    'owner': 'airflow',
    'start_date' : datetime(2024,1,1), # 날짜가 미래인 경우 실행이 안됨 
    'schedule_interval' : '0 4 1 * *',  # 매달 1일 4시에 실행
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'gradulation_schedule_gcs_to_bigquery_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:

    load_to_bigquery = PythonOperator(
        task_id='gradulation_schedule_gcs_to_bigquery',
        python_callable=gradulation_schedule_gcs_to_bigquery,
    )