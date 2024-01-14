import sys
import os
import numpy as np

import math

#판다스
import pandas as pd

#HTML문서 분석 라이브러리
from bs4 import BeautifulSoup

#URL 파싱
from urllib.request import urlopen
from urllib import parse
import requests

# 현재 날짜 시간 가져오기
from datetime import datetime, timedelta
from time import strftime
import datetime as dt
from pytz import timezone

import logging

#스케줄러
import time

import xml.etree.ElementTree as ET

#airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from io import BytesIO

import warnings
warnings.filterwarnings('ignore')

def call_api(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        xml_data = response.content.decode("utf-8")
        root = ET.fromstring(xml_data)
        return root.findall(".//row")
    except requests.exceptions.HTTPError as http_err:
        logging.error('[API 호출 실패][{}] {}'.format(str(response), str(http_err)))
    except Exception as e:
        logging.error('[API 호출 실패][{}] {}'.format(str(response), str(e)))


def get_academic_data(url):
    data = []
    rows = call_api(url)
    for row in rows:
        event_data = {}
        # Extract values from each row
        event_data['시도교육청코드'] = row.find("ATPT_OFCDC_SC_CODE").text
        event_data['시도교육청명'] = row.find("ATPT_OFCDC_SC_NM").text
        event_data['행정표준코드'] = row.find("SD_SCHUL_CODE").text
        event_data['학교명'] = row.find("SCHUL_NM").text
        event_data['학년도'] = row.find("AY").text
        event_data['주야과정명'] = row.find("DGHT_CRSE_SC_NM").text
        event_data['학교과정명'] = row.find("SCHUL_CRSE_SC_NM").text
        event_data['수업공제일명'] = row.find("SBTR_DD_SC_NM").text
        event_data['학사일자'] = row.find("AA_YMD").text
        event_data['행사명'] = row.find("EVENT_NM").text
        event_data['행사내용'] = row.find("EVENT_CNTNT").text
        event_data['1학년행사여부'] = row.find("ONE_GRADE_EVENT_YN").text
        event_data['2학년행사여부'] = row.find("TW_GRADE_EVENT_YN").text
        event_data['3학년행사여부'] = row.find("THREE_GRADE_EVENT_YN").text
        event_data['4학년행사여부'] = row.find("FR_GRADE_EVENT_YN").text
        event_data['5학년행사여부'] = row.find("FIV_GRADE_EVENT_YN").text
        event_data['6학년행사여부'] = row.find("SIX_GRADE_EVENT_YN").text
        event_data['수정일자'] = row.find("LOAD_DTM").text
        data.append(event_data)

    return data

def academic_calendar_api_to_gcs(**kwargs):

    start_date = kwargs.get("ds")

    logging.info(f'start_date : {start_date}')
    # 날짜 형식으로 변환
    start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    start_date = start_date_datetime.strftime("%Y%m%d")
    # 한 달을 더하고 하루를 빼서 마지막 날을 구함
    end_date_datetime = (start_date_datetime.replace(day=1) + timedelta(days=31)).replace(day=1) - timedelta(days=1)
    # 결과를 원하는 형식으로 출력
    end_date = end_date_datetime.strftime("%Y%m%d")

    # GCSHook을 사용하여 GCS에 파일 읽기-------------------------------
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn') # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket' # 버킷이름
    object_name = 'academic_calendar/code.csv'
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)

    # BytesIO를 사용하여 파일 내용을 pandas DataFrame으로 변환
    df_code = pd.read_csv(BytesIO(file_content), encoding=file_encoding)
    # ------------------------------------------------------------------

    df_data = pd.DataFrame(columns=DF_COLS)
    for index, row in df_code.iterrows():
        education_code = row['시도교육청코드']
        administrative_code = row['행정표준코드']
        academic_calendar_api_key = Variable.get("academic_calendar_api_key")

        api_url = f'https://open.neis.go.kr/hub/SchoolSchedule?KEY={academic_calendar_api_key}&Type={TYPE}&pIndex={PINDEX}&pSize={PSIZE}&ATPT_OFCDC_SC_CODE={education_code}&SD_SCHUL_CODE={administrative_code}&AA_FROM_YMD={start_date}&AA_TO_YMD={end_date}'
        data = get_academic_data(api_url)

        if len(data) > 0:
            df_data = df_data.append(data)

    # CSV 파일로 저장
    logging.info(f'{start_date} : {len(df_data)}')
    csv_string = df_data.to_csv(index=False, encoding=file_encoding, errors='ignore')

    # GCSHook을 사용하여 GCS에 파일 업로드-------------------------------
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn') # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket' # 버킷이름
    object_name = f'/academic_calendar/학사일정_{start_date[:4]}년{start_date[4:6]}월.csv'
    gcs_hook.upload(bucket_name, object_name, data=csv_string, mime_type='text/csv') # 업로드
    # ------------------------------------------------------------------

# OPENAPI 설정값
TYPE = "XML"  # XML, JSON
PINDEX = "1"
PSIZE = "1000"

# COLUMNS
DF_COLS = ["시도교육청코드", "시도교육청명", "행정표준코드", "학교명", "학년도", "주야과정명", "학교과정명", "수업공제일명", "학사일자", "행사명", "행사내용", "1학년행사여부",
           "2학년행사여부", "3학년행사여부", "4학년행사여부", "5학년행사여부", "6학년행사여부", "수정일자"]

file_encoding = 'cp949'

# DAG 정의
dag = DAG(
    dag_id = 'academic_calendar_api_to_gcs_dag',
    start_date = datetime(2024,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 0 1 * *',  # 매달 1일 0시에 실행
    catchup = True,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

task = PythonOperator(
    task_id = 'academic_calendar_api_to_gcs',
    python_callable = academic_calendar_api_to_gcs,
    provide_context=True,
    dag = dag)