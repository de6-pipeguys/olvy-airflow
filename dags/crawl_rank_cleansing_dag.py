from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import json
import pandas as pd
from seleniumbase import SB
from crawlers.crawl_rank_cleansing import get_top100_cleansing, get_product_detail_info
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import boto3
import logging

def crawl_cleansing_data(**context):
    logging.info("get_top100_cleansing 실행")
    data, goods_no_list = get_top100_cleansing()

    logging.info(f"Top100 상품 수: {len(goods_no_list)}")

    detail_list = []
    with SB(uc=True, test=True, headless=True) as sb:
        for goods_no in goods_no_list:
            try:
                detail = get_product_detail_info(sb, goods_no)
                detail_list.append(detail)
            except Exception as e:
                logging.warning(f"상세 정보 수집 실패: {goods_no} | {e}")
                detail_list.append({})

    logging.info("데이터 병합 및 저장")
    df_basic = pd.DataFrame(data)
    df_detail = pd.DataFrame(detail_list)
    result_df = pd.concat([df_basic.reset_index(drop=True), df_detail.reset_index(drop=True)], axis=1)

    # 저장 경로 지정
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/airflow/data/cleansing_{ts}.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_json(output_path, orient="records", force_ascii=False, indent=2)

    context['ti'].xcom_push(key='cleansing_file_path', value=output_path)
    logging.info(f"JSON 저장 완료!: {output_path}")

def upload_to_s3(**context):
    file_path = context['ti'].xcom_pull(key='cleansing_file_path', task_ids='crawl_cleansing')
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw_data/non_pb/cleansing/{ts}.json"
    bucket_name = "de6-final-test"

    try:
        hook = S3Hook(aws_conn_id='test_s3')  
        hook.load_file(filename=file_path, key=s3_key, bucket_name=bucket_name, replace=True)
        logging.info(f"S3 업로드 성공: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"S3 업로드 실패: {e}")
        raise

# =======  DAG 정의 =======
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id="crawl_cleansing_dag",
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval = None,
    catchup=False,
) as dag:

    crawl_cleansing = PythonOperator(
        task_id="crawl_cleansing",
        python_callable=crawl_cleansing_data,
        provide_context=True,
    )

    upload_json_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    crawl_cleansing >> upload_json_to_s3
