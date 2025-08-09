from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
from seleniumbase import SB
import os
from crawler import crawl_brand_suncare
from plugins import slack
from airflow.sdk import Variable
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack.on_failure_callback,
    'on_retry_callback': slack.on_retry_callback,
}

# 예상 소요시간 - 한 브랜드 당 20분
PB_BRAND_CODE_DICT = {
    "식물나라": "A000036"
}

def crawl_pb_brand(**context):
    brand_data = {}
    fail_brands = {}
    for brand_name, brand_code in PB_BRAND_CODE_DICT.items():
        data, goods_no_list = crawl_brand_suncare.get_brand(brand_name, brand_code)
        if not goods_no_list or len(goods_no_list) == 0:
            fail_brands.append(brand_name)
        brand_data[brand_name] = {
            "data": data,
            "goods_no_list": goods_no_list
        }
    # 일부 실패한 경우 경고 로그
    if fail_brands:
        raise ValueError(f"다음 브랜드 크롤링 실패: {fail_brands}")
    context['ti'].xcom_push(key='brand_data', value=brand_data)

def crawl_pb_product_info(**context):
    from datetime import datetime
    brand_data = context['ti'].xcom_pull(key='brand_data', task_ids='crawl_pb_brand')
    timestamp = datetime.now().strftime("%Y%m%d")
    filenames = []
    for brand_name, brand_info in brand_data.items():
        data = brand_info["data"]
        goods_no_list = brand_info["goods_no_list"]
        logging.info(f"{brand_name} 데이터 수집 시작")
        detail_list = []
        with SB(uc=True, test=True, headless=True) as sb:
            for idx, goods_no in enumerate(goods_no_list, 1):
                detail = crawl_brand_suncare.get_brand_product_detail_info(sb, goods_no)
                detail_list.append(detail)
                logging.info(f"{idx}번째 데이터 수집 완료")

        if len(detail_list) < len(goods_no_list):
            raise ValueError("랭킹 상품 데이터를 모두 수집하지 못했습니다. 수집 과정에서 오류가 발생했습니다.")
        else:
            logging.info(f"{len(detail_list)}개의 상품데이터가 정상적으로 수집되었습니다.")

        df = pd.DataFrame(data)
        detail_df = pd.DataFrame(detail_list)
        result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)
        # 파일명 생성
        brand_file_key = brand_name.lower().replace(" ", "_").replace("식물나라", "shingmulnara")
        filename = f"pb_{brand_file_key}_result_{timestamp}_130000.json"
        result_df.to_json(filename, orient='records', force_ascii=False, indent=2)
        filenames.append(filename)
    context['ti'].xcom_push(key='result_filenames', value=filenames)

def upload_to_s3(**context):
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ap-northeast-2'
    )
    filenames = context['ti'].xcom_pull(key='result_filenames', task_ids='crawl_pb_product_info')
    for filename in filenames:
        # 파일명에서 브랜드 키 추출 (예: pb_biohealboh_result_20250708_081022.json → biohealboh)
        brand_key = filename.split('_')[1]
        s3_object_name = f"raw_data/pb/{brand_key}/{filename}"
        try:
            s3.upload_file(filename, 'de6-team5-bucket', s3_object_name)
            logging.info(f"{filename} 파일이 S3에 정상적으로 저장되었습니다. (경로: {s3_object_name})")
        except Exception as e:
            logging.error(f"{filename} 파일 S3 업로드 실패: {e}")
            raise
        if os.path.exists(filename):
            os.remove(filename)
            logging.info(f"{filename} 파일이 로컬에서 삭제되었습니다.")

with DAG(
    dag_id='pb_brand_crawl_suncare',
    default_args=default_args,
    description='PB 브랜드 전체 데이터 수집',
    #schedule_interval="5 16 * * *",  # airflow 2 버전
    schedule="0 13 * * *",        # airflow 3 버전
    start_date=datetime(2024, 7, 1),
    catchup=False,
    tags=['pb_brand', 'suncare'],
) as dag:

    crawl_pb_brand = PythonOperator(
        task_id='crawl_pb_brand',
        python_callable=crawl_pb_brand,
        #provide_context=True,
    )

    crawl_pb_product_info = PythonOperator(
        task_id='crawl_pb_product_info',
        python_callable=crawl_pb_product_info,
        #provide_context=True,
    )

    upload_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable=upload_to_s3,
        #provide_context=True,
    )

    crawl_pb_brand >> crawl_pb_product_info >> upload_s3
