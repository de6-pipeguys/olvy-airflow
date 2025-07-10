from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime as dt, timedelta
import os
import json
import pandas as pd
from seleniumbase import SB
from plugins.crawl_rank import get_top100, get_product_detail_info
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# import boto3
import logging
from pendulum import datetime, timezone

def crawl_suncare_data(**context):
    logging.info("get_top100_suncare 실행")
    
    # 올리브영 선케어 랭킹 페이지 열기
    url = "https://www.oliveyoung.co.kr/store/main/getBestList.do?dispCatNo=900000100100001&fltDispCatNo=10000010011&pageIdx=1&rowsPerPage=8"
    data, goods_no_list = get_top100(url)

    logging.info(f"Top100 상품 수: {len(goods_no_list)}")

    detail_list = []


    BATCH_SIZE = 50  # 🔁 50개 단위로 크롬 재시작
    total = len(goods_no_list)

    for start in range(0, total, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total)
        logging.info(f"크롬 인스턴스 새로 시작: {start+1} ~ {end}위")

        with SB(uc=True, test=True, headless=True) as sb:
            for idx in range(start, end):
                goods_no = goods_no_list[idx]
                logging.info(f"[{idx + 1}위] 상세정보 크롤링 시작 - goodsNo: {goods_no}")
                try:
                    detail = get_product_detail_info(sb, goods_no)
                    detail_list.append(detail)
                    logging.info(f"[{idx + 1}위] 크롤링 성공 - goodsNo: {goods_no}")
                except Exception as e:
                    logging.warning(f"[{idx + 1}위] 크롤링 실패 - goodsNo: {goods_no} | 에러: {e}")
                    detail_list.append({})

    logging.info("데이터 병합 및 저장")
    df_basic = pd.DataFrame(data)
    df_detail = pd.DataFrame(detail_list)
    result_df = pd.concat([df_basic.reset_index(drop=True), df_detail.reset_index(drop=True)], axis=1)

    # 저장 경로 지정
    ts = dt.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/opt/airflow/data/suncare/suncare_{ts}.json"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    result_df.to_json(output_path, orient="records", force_ascii=False, indent=2)

    context['ti'].xcom_push(key='suncare_file_path', value=output_path)
    logging.info(f"JSON 저장 완료!: {output_path}")

def upload_to_s3(**context):
    file_path = context['ti'].xcom_pull(key='suncare_file_path', task_ids='crawl_suncare')
    ts = dt.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw_data/non_pb/suncare/rank_suncare_result_{ts}.json"
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
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2025, 7, 1, tz=timezone("Asia/Seoul"))
}

with DAG(
    dag_id="crawl_suncare_morning_dag",
    default_args=default_args,
    schedule_interval = "30 9 * * *",
    catchup=False,
) as dag_morning:

    crawl_suncare = PythonOperator(
        task_id="crawl_suncare",
        python_callable=crawl_suncare_data,
        provide_context=True,
    )

    upload_json_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    crawl_suncare >> upload_json_to_s3

with DAG(
    dag_id="crawl_suncare_evening_dag",
    default_args=default_args,
    schedule_interval = "1 17 * * *",
    catchup=False,
) as dag_evening:

    crawl_suncare = PythonOperator(
        task_id="crawl_suncare",
        python_callable=crawl_suncare_data,
        provide_context=True,
    )

    upload_json_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
        provide_context=True,
    )

    crawl_suncare >> upload_json_to_s3

