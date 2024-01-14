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

def gradulation_schedule_update_on_gcs(**kwargs):

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
    object_name = f'academic_calendar/학사일정_{start_date[:4]}년{start_date[4:6]}월.csv'
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)

    # BytesIO를 사용하여 파일 내용을 pandas DataFrame으로 변환
    df_data = pd.read_csv(BytesIO(file_content), encoding=file_encoding)

    object_name = 'academic_calendar/gradulation_schedule.csv'
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)

    # BytesIO를 사용하여 파일 내용을 pandas DataFrame으로 변환
    df_gradulation_schedule_pre = pd.read_csv(BytesIO(file_content), encoding=file_encoding)

    AA_FROM_YMD = f'{start_date[0:4]}-{start_date[4:6]}-{start_date[6:8]}'
    AA_TO_YMD = f'{end_date[0:4]}-{end_date[4:6]}-{end_date[6:8]}'

    date_range = pd.date_range(start=AA_FROM_YMD, end=AA_TO_YMD, freq="D")
    df_gradulation_schedule = pd.DataFrame({"date": date_range})

    # "행사명"이 '졸업식'을 포함하고, "학사일자"가 NaN이 아닌 행만 선택
    graduation_date_list = df_data[df_data["행사명"].str.contains('졸업식') & ~df_data["학사일자"].isna()]["학사일자"]
    df_graduation_date_list = pd.DataFrame(graduation_date_list)

    # "학자일자" 열의 날짜 포맷 변경
    df_graduation_date_list["학사일자"] = pd.to_datetime(df_graduation_date_list["학사일자"], format="%Y%m%d")
    df_graduation_date_list = df_graduation_date_list.rename(columns={"학사일자": "date"})

    df_graduation_date_list = df_graduation_date_list["date"].value_counts().reset_index()
    df_graduation_date_list.columns = ["date", "event_count"]

    df_gradulation_schedule = pd.merge(df_gradulation_schedule, df_graduation_date_list, on="date", how="left").fillna(0)

    # 기존 졸업식 행사 리스트와 병합
    df_gradulation_schedule = pd.concat([df_gradulation_schedule, df_gradulation_schedule_pre], ignore_index=True)

    # 중복된 값 제거
    df_gradulation_schedule = df_gradulation_schedule.drop_duplicates(subset='date')

    # CSV 파일로 저장
    logging.info('gradulation_schedule.csv 파일 저장')
    csv_string = df_gradulation_schedule.to_csv(index=False, encoding=file_encoding, errors='ignore')

    # GCSHook을 사용하여 GCS에 파일 업로드-------------------------------
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn') # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket' # 버킷이름
    object_name = f'/academic_calendar/gradulation_schedule.csv'
    gcs_hook.upload(bucket_name, object_name, data=csv_string, mime_type='text/csv') # 업로드
    # ------------------------------------------------------------------


def event_schedule_update_on_gcs(**kwargs):
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
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn')  # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket'  # 버킷이름
    object_name = 'academic_calendar/gradulation_schedule.csv'
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)

    # BytesIO를 사용하여 파일 내용을 pandas DataFrame으로 변환
    df_gradulation_schedule = pd.read_csv(BytesIO(file_content), encoding=file_encoding)

    object_name = 'academic_calendar/event_list.csv'
    file_content = gcs_hook.download(bucket_name=bucket_name, object_name=object_name)

    # BytesIO를 사용하여 파일 내용을 pandas DataFrame으로 변환
    df_event_schedule= pd.read_csv(BytesIO(file_content), encoding=file_utf8_encoding)

    df_gradulation_schedule["event_name"] = "졸업식"
    df_gradulation_schedule = df_gradulation_schedule.rename(columns={"date": "event_date"})
    df_gradulation_schedule = df_gradulation_schedule[["event_date", "event_name", "event_count"]]
    df_event_schedule = pd.concat([df_event_schedule, df_gradulation_schedule], ignore_index=True)
    df_event_schedule = df_event_schedule.sort_values(by="event_date")
    df_event_schedule = df_event_schedule[df_event_schedule["event_count"] > 0]

    # 중복된 값 제거
    df_event_schedule = df_event_schedule.drop_duplicates(subset='event_date')

    # CSV 파일로 저장
    logging.info('event_list.csv 파일 저장')
    csv_string = df_event_schedule.to_csv(index=False, encoding=file_utf8_encoding, errors='ignore')

    # GCSHook을 사용하여 GCS에 파일 업로드-------------------------------
    gcs_hook = GCSHook(gcp_conn_id='gcp_conn')  # Airflow 웹 UI connection 정보 참조(서비스 계정 키 설정)
    bucket_name = 'flower-pipeline-bucket'  # 버킷이름
    object_name = f'/academic_calendar/event_list.csv'
    gcs_hook.upload(bucket_name, object_name, data=csv_string, mime_type='text/csv')  # 업로드
    # ------------------------------------------------------------------

file_encoding = 'cp949'
file_utf8_encoding = 'utf-8'

# DAG 정의
dag = DAG(
    dag_id = 'academic_calendar_update_on_gcs_dag',
    start_date = datetime(2024,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='0 3 1 * *',  # 매달 1일 3시에 실행
    catchup = True,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

gradulation_schedule_update_on_gcs = PythonOperator(
    task_id = 'gradulation_schedule_update_on_gcs',
    python_callable = gradulation_schedule_update_on_gcs,
    provide_context=True,
    dag = dag)

event_schedule_update_on_gcs = PythonOperator(
    task_id = 'event_schedule_update_on_gcs',
    python_callable = event_schedule_update_on_gcs,
    provide_context=True,
    dag = dag)

#Assign the order of the tasks in our DAG
gradulation_schedule_update_on_gcs >> event_schedule_update_on_gcs