# [Programmers-devcourse]
### 데이터 웨어하우스를 이용한 대시보드 구성<br><br>
## 🌻주제<br>
### 화훼가격과 거시지표를 이용한 데이터 분석
ETL 프로세스를 통해 데이터웨어하우스에 데이터를 적재하고 대시보드 구성해 보는 프로젝트
<br/>
<br/>
## 🌻목표
화훼 데이터를 활용하여 __`일자별 화훼 가격 대시보드`__ 를 생성하는 것을 목표로 합니다.<br>
데이터 수집, 전처리, GCS에 저장 및 Bigquery 적재를 자동화 하는 __End-to-end 파이프라인을 경험__ 하고, <br>
데이터웨어하우스에 적재된 데이터를 이용하여 __대시보드를 생성__ 합니다.<br><br>
## 🌻기대효과
Python, SQL 언어로 데이터를 전처리할 수 있습니다.<br>
GCP(GCS, Bigquery, VM)와 Airflow를 이용하여 데이터 파이프라인을 설계하고 구현할 수 있습니다.<br>
클라우드 서버를 이용하여 Docker 컨테이너로 Airflow를 관리할 수 있습니다.<br>
Airflow dag를 깃허브를 이용해 관리할 수 있습니다.<br>
데이터 웨어하우스(Bigquery)를 활용하여 대용량 파일을 처리할 수  있습니다.<br>
데이터를 분석하고 이를 시각적으로 표현하는 데이터 대시보드(Superset)를 만들 수 있습니다.<br><br>

------------

## 🌼사용 데이터
- [일자별 화훼 경매 정보](https://flower.at.or.kr/api/apiOpenInfo.do) <br>
- [기상청 데이터](https://data.kma.go.kr/data/grnd/selectAsosRltmList.do?pgmNo=36&tabNo=2) <br>
- [학사일정](https://open.neis.go.kr/portal/data/service/selectServicePage.do?page=1&rows=10&sortColumn=&sortDirection=&infId=OPEN17220190722175038389180&infSeq=2)<br><br>
## 🌼프로젝트 구조
![Untitled-2024-01-13-0010 excalidraw](https://github.com/es3442/ETL_Airflow_Flower/assets/77157003/7ddc9160-e4d9-49e1-8b72-7a76dbc02976)<br>
1. API 호출을 통한 __데이터 수집 및 데이터 전처리__
2. __클라우드 스토리지에 데이터 저장__ (Google Cloud Storage)
3. Google cloud storage에 저장된 파일을 __데이터 웨어하우스에 적재__ (Bigquery)
4. 데이터 웨어하우스(Bigquery) 대시보드(Superset)에 연결
5. __대시보드 생성 및 데이터 분석__ <br><br>
## 🌼사용 기술 및 주요 프레임워크
### 언어
- Python, SQL
### Cloud Storage
- Google Storage<br>
### Data Warehouse
- Big Query<br>
### Scheduling
- Airflow<br>
### 시각화
- Superset(preset.io)<br>
### 협업
- GitHub
- Slack<br><br>

-----------
## 🫵역할
### 인프라 관리
- 김바롬 : GCP(GCS, Bigquery) 구축
- 임동빈 : Preset 구축
- 최은서 : GCP(VM)를 사용하여 Airflow서버 구축
### ETL 프로세스(데이터 수집, 전처리, 클라우드 스토리지 저장, Bigquery 적재)
- 김바롬 : 기본 코드 제공
- 임동빈 : `기상청 데이터` - [Airflow dag로 GCS 저장 자동화](https://github.com/es3442/ETL_Airflow_Flower/blob/main/dags/weather_api_etl.py), [Airflow GCSToBigQueryOperator로 GCS -> BigQuery 적재 자동화](https://github.com/es3442/ETL_Airflow_Flower/blob/main/dags/weather_bucket_to_BQ.py)
- 최윤주 : `학교별 일정 데이터` - [Airflow dag로 GCS 저장 자동화](dags/academic_calendar_API_to_GCS.py), [Airflow dag로 GCS->Bigquery 적재 자동화](dags/EventScheduleGCSToBigQuery.py)
- 최은서 : `화훼 데이터` - [Airflow dag로 GCS 저장 자동화](https://github.com/es3442/ETL_Airflow_Flower/blob/main/dags/fetch_and_upload_dag_final.py), [Bigquery-Data_Transfer로 GCS->Bigquery 적재 자동화]
### ELT 및 데이터 분석(대시보드 작성)
- 임동빈 
- 최봉승
- 최윤주
