from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from seleniumbase import SB
import logging
from crawlers.crawl_brand import get_brand, get_brand_product_detail_info
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def crawl_careplus_with_detail(**context):
    brand_code = "A003339"
    brand_name = "케어플러스"
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    data, goods_no_list = get_brand(brand_name, brand_code)
    df = pd.DataFrame(data)


    logging.info(f"✅ 수집된 제품 수: {len(df)}개")

    if df.empty:
        logging.warning("❌ 크롤링 결과가 비었습니다")
        return

    with SB(uc=True, test=True, headless=True) as sb:
        detail_list = []
        for idx, row in df.iterrows():
            goods_no = row['goodsNo']
            # product_name = row.get('goodsName', '이름없음')

            logging.info(f"[{idx+1}/{len(df)}] 상세정보 크롤링 시작 - goodsNo: {goods_no}")
            try:
                detail = get_brand_product_detail_info(sb, goods_no)
                detail_list.append(detail)
                logging.info(f"[{idx+1}/{len(df)}] 크롤링 성공 - goodsNo: {goods_no}")
            except Exception as e:
                logging.warning(f"'[{idx+1}/{len(df)}] 크롤링 실패 - goodsNo: {goods_no} | 에러: {e}")
                detail_list.append({})  # 실패해도 빈 값이라도 넣어주기

    detail_df = pd.DataFrame(detail_list)
    result_df = pd.concat([df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1)

    filename = f"careplus_{ts}.json"
    local_path = f"/opt/airflow/data/{filename}"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    result_df.to_json(local_path, orient='records', force_ascii=False, indent=2)
    logging.info(f"✅ 저장 파일명: {local_path}")

    context['ti'].xcom_push(key='local_path', value=local_path)
    context['ti'].xcom_push(key='s3_key', value=f"raw_data/pb/{filename}")
    print(f"저장 완료: {local_path}")

def upload_to_s3(**context):
    local_path = context['ti'].xcom_pull(key='local_path')
    s3_key = context['ti'].xcom_pull(key='s3_key')

    s3_hook = S3Hook(aws_conn_id="test_s3")
    bucket_name = "de6-final-test"  # 실제 버킷명으로 바꿔야 함

    s3_hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"S3 업로드 완료: s3://{bucket_name}/{s3_key}")

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='careplus_crawling_dag',
    default_args=default_args,
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_crawl = PythonOperator(
        task_id='crawl_pbbrand_careplus',
        python_callable=crawl_careplus_with_detail,
        provide_context=True
    )

    task_upload = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )

    task_crawl >> task_upload