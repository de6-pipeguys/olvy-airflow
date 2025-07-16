from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from seleniumbase import SB
import os
import logging

from crawlers import crawl_rank
from utils import slack
from airflow.models import Variable
from pendulum import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback,
    'on_retry_callback': slack.on_retry_callback, 
}

##아침용
def crawl_top100_food_morning(**context):
    logging.info("[crawl_top100_food_morning] Food 랭킹 크롤링 시작")
    #get_top100 함수에 category="food"를 전달
    data, goods_no_list = crawl_rank.get_top100(category="food") 
    logging.info(f"[crawl_top100_food_morning] Food 랭킹 {len(data)}개 상품 데이터 수집 완료")
    context['ti'].xcom_push(key='top100_data', value=data)
    context['ti'].xcom_push(key='goods_no_list', value=goods_no_list)

def crawl_product_info_morning(**context):
    data = context['ti'].xcom_pull(key='top100_data', task_ids='crawl_top100')
    goods_no_list = context['ti'].xcom_pull(key='goods_no_list', task_ids='crawl_top100')
    df = pd.DataFrame(data)

    with SB(uc=True, test=True, headless=True) as sb:
        detail_list = []
        for idx, goods_no in enumerate(goods_no_list, 1):
            detail = crawl_rank.get_product_detail_info(sb, goods_no)
            detail_list.append(detail)
            logging.info(f"{idx}번째 데이터 수집 완료")

    if len(detail_list) != 100:
        raise ValueError("랭킹 상품 데이터를 모두 수집하지 못했습니다. 수집 과정에서 오류가 발생했습니다.")
    else:
        logging.info("100개의 상품데이터가 정상적으로 수집되었습니다.")

    detail_df = pd.DataFrame(detail_list)
    result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rank_food_result_{timestamp}.json"
    result_df.to_json(filename, orient='records', force_ascii=False, indent=2)
    context['ti'].xcom_push(key='result_filename', value=filename)

# 실제 S3 업로드 함수
def upload_to_s3_morning(**context):
    aws_access_key_id = Variable.get("YOUR_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("YOUR_AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ap-northeast-2'
    )
    # XCom에서 파일명 가져오기
    filename = context['ti'].xcom_pull(key='result_filename', task_ids='crawl_product')
    # S3 object 이름도 타임스탬프 포함
    s3_object_name = f"raw_data/non_pb/food/{filename}"
    s3.upload_file(filename, 'de6-team5-bucket', s3_object_name)
    # (선택) 업로드 후 파일 삭제
    if os.path.exists(filename):
        os.remove(filename)


## 저녁용
def crawl_top100_food_afternoon(**context):
    logging.info("[crawl_top100_food_afternoon] Food 랭킹 크롤링 시작")
    #get_top100 함수에 category="food"를 전달
    data, goods_no_list = crawl_rank.get_top100(category="food") 
    logging.info(f"[crawl_top100_food_afternoon] Food 랭킹 {len(data)}개 상품 데이터 수집 완료")
    context['ti'].xcom_push(key='top100_data', value=data)
    context['ti'].xcom_push(key='goods_no_list', value=goods_no_list)

def crawl_product_info_afternoon(**context):
    data = context['ti'].xcom_pull(key='top100_data', task_ids='crawl_top100_afternoon')
    goods_no_list = context['ti'].xcom_pull(key='goods_no_list', task_ids='crawl_top100_afternoon')
    df = pd.DataFrame(data)

    with SB(uc=True, test=True, headless=True) as sb:
        detail_list = []
        for idx, goods_no in enumerate(goods_no_list, 1):
            detail = crawl_rank.get_product_detail_info(sb, goods_no)
            detail_list.append(detail)
            logging.info(f"{idx}번째 데이터 수집 완료")

    if len(detail_list) != 100:
        raise ValueError("랭킹 상품 데이터를 모두 수집하지 못했습니다. 수집 과정에서 오류가 발생했습니다.")
    else:
        logging.info("100개의 상품데이터가 정상적으로 수집되었습니다.")

    detail_df = pd.DataFrame(detail_list)
    result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rank_food_result_{timestamp}.json"
    result_df.to_json(filename, orient='records', force_ascii=False, indent=2)
    context['ti'].xcom_push(key='result_filename', value=filename)

# 실제 S3 업로드 함수
def upload_to_s3_afternoon(**context):
    aws_access_key_id = Variable.get("YOUR_AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("YOUR_AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ap-northeast-2'
    )
    # XCom에서 파일명 가져오기
    filename = context['ti'].xcom_pull(key='result_filename', task_ids='crawl_product_afternoon')
    # S3 object 이름도 타임스탬프 포함
    s3_object_name = f"raw_data/non_pb/food/{filename}"
    s3.upload_file(filename, 'de6-team5-bucket', s3_object_name)
    # (선택) 업로드 후 파일 삭제
    if os.path.exists(filename):
        os.remove(filename)


with DAG(
    dag_id='food_crawl_morning',
    default_args=default_args,
    description='food ranking crawl(morning)',
    #schedule_interval="30 0 * * *",  # 한국시간 오전 9시 30분 (UTC 0시 30분)

    schedule="30 9 * * *",        # airflow 3 버전
    start_date=datetime(2024, 7, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['food', 'ranking','morning'],
) as dag:

    crawl_top100 = PythonOperator(
        task_id='crawl_top100',
        python_callable=crawl_top100_food_morning,
        #provide_context=True,
    )

    crawl_product = PythonOperator(
        task_id='crawl_product',
        python_callable=crawl_product_info_morning,
        #provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_to_s3_morning,
        #provide_context=True,
    )

    crawl_top100 >> crawl_product >> upload_s3

with DAG(
    dag_id='food_crawl_afternoon',
    default_args=default_args,
    description='food ranking crawl(afternoon)',
    #schedule_interval="1 8 * * *",   # 한국시간 오후 5시 1분 (UTC 8시 1분)
    schedule="1 17 * * *",        # airflow 3 버전
    start_date=datetime(2024, 7, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['food', 'ranking','afternoon'],
) as dag:

    crawl_top100_afternoon = PythonOperator(
        task_id='crawl_top100_afternoon',
        python_callable=crawl_top100_food_afternoon,
        #provide_context=True,
    )

    crawl_product_afternoon = PythonOperator(
        task_id='crawl_product_afternoon',
        python_callable=crawl_product_info_afternoon,
        #provide_context=True,
    )

    upload_s3_afternoon = PythonOperator(
        task_id='upload_s3_afternoon',
        python_callable=upload_to_s3_afternoon,
        #provide_context=True,
    )

    crawl_top100_afternoon >> crawl_product_afternoon >> upload_s3_afternoon
