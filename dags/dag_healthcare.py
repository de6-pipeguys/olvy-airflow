from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from seleniumbase import SB
import chromedriver_autoinstaller
import os
import logging

from crawler import crawl_rank_healthcare
from plugins import slack
from airflow.sdk import Variable
from pendulum import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback,
    'on_retry_callback': slack.on_retry_callback,
}

def crawl_top100_healthcare(**context):
    chromedriver_autoinstaller.install()
    data, goods_no_list = crawl_rank_healthcare.get_top100_healthcare()
    context['ti'].xcom_push(key='top100_data', value=data)
    context['ti'].xcom_push(key='goods_no_list', value=goods_no_list)


def crawl_product_info(**context):
    data = context['ti'].xcom_pull(key='top100_data', task_ids='crawl_top100')
    goods_no_list = context['ti'].xcom_pull(key='goods_no_list', task_ids='crawl_top100')
    df = pd.DataFrame(data)

    chromedriver_autoinstaller.install()

    with SB(uc=True, test=True, headless=True) as sb:
        detail_list = []
        for idx, goods_no in enumerate(goods_no_list, 1):
            detail = crawl_rank_healthcare.get_product_detail_info(sb, goods_no)
            detail_list.append(detail)
            logging.info(f"{idx}번째 데이터 수집 완료")

    if len(detail_list) < 100:
        raise ValueError("랭킹 상품 데이터를 모두 수집하지 못했습니다. 수집 과정에서 오류가 발생했습니다.")
    else:
        logging.info("100개의 상품데이터가 정상적으로 수집되었습니다.")

    detail_df = pd.DataFrame(detail_list)
    result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)
    timestamp = datetime.now().strftime("%Y%m%d")
    dag_id = context['dag'].dag_id
    #filename = f"rank_healthcare_result_{timestamp}_%H%M%S.json"
    if dag_id == 'rank_crawl_afternoon_healthcare':
        time_str = "170100"
    elif dag_id == 'rank_crawl_morning_healthcare':
        time_str = "093000"
    else:
        time_str = "000000"

    filename = f"rank_healthcare_result_{timestamp}_{time_str}.json"
    result_df.to_json(filename, orient='records', force_ascii=False, indent=2)
    context['ti'].xcom_push(key='result_filename', value=filename)

# 실제 S3 업로드 함수
def upload_to_s3(**context):
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ap-northeast-2'
    )
    # XCom에서 파일명 가져오기
    filename = context['ti'].xcom_pull(key='result_filename', task_ids='crawl_product')
    # S3 object 이름도 타임스탬프 포함
    s3_object_name = f"raw_data/non_pb/healthcare/{filename}"
    s3.upload_file(filename, 'de6-team5-bucket', s3_object_name)
    # (선택) 업로드 후 파일 삭제
    if os.path.exists(filename):
        os.remove(filename)


with DAG(
    dag_id='rank_crawl_morning_healthcare',
    default_args=default_args,
    description='healthcare ranking crawl(morning)',
    #schedule_interval="5 16 * * *",  # airflow 2 버전
    schedule="30 9 * * *",        # airflow 3 버전
    start_date=datetime(2024, 7, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['healthcare', 'ranking', 'morning'],
) as dag:

    crawl_top100 = PythonOperator(
        task_id='crawl_top100',
        python_callable=crawl_top100_healthcare,
        #provide_context=True,
    )

    crawl_product = PythonOperator(
        task_id='crawl_product',
        python_callable=crawl_product_info,
        #provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_to_s3,
        #provide_context=True,
    )

    crawl_top100 >> crawl_product >> upload_s3

with DAG(
    dag_id='rank_crawl_afternoon_healthcare',
    default_args=default_args,
    description='healthcare ranking crawl(afternoon)',
    #schedule_interval="5 16 * * *",  # airflow 2 버전
    schedule="1 17 * * *",        # airflow 3 버전
    start_date=datetime(2024, 7, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['healthcare', 'ranking', 'afternoon'],
) as dag:

    crawl_top100 = PythonOperator(
        task_id='crawl_top100',
        python_callable=crawl_top100_healthcare,
        #provide_context=True,
    )

    crawl_product = PythonOperator(
        task_id='crawl_product',
        python_callable=crawl_product_info,
        #provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_to_s3,
        #provide_context=True,
    )

    crawl_top100 >> crawl_product >> upload_s3

