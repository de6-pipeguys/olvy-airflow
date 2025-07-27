from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from seleniumbase import SB
from crawler.crawl_rank_suncare import get_top100, get_rank_detail_info
from plugins import slack
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
import pendulum
from airflow.exceptions import AirflowFailException


def crawl_suncare_data(**context):
    logging.info("get_top100_suncare 실행")

    # 올리브영 선케어 랭킹 페이지 열기
    url = "https://www.oliveyoung.co.kr/store/main/getBestList.do?dispCatNo=900000100100001&fltDispCatNo=10000010011&pageIdx=1&rowsPerPage=8"
    data, goods_no_list = get_top100(url)

    logging.info(f"Top100 상품 수: {len(goods_no_list)}")

    detail_list = []
    failed_goods = []
    BATCH_SIZE = 50  # 🔁 50개 단위로 크롬 재시작
    total = len(goods_no_list)

    for start in range(0, total, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total)
        logging.info(f"크롬 인스턴스 새로 시작: {start + 1} ~ {end}위")

        with SB(uc=True, test=True, headless=True) as sb:
            for idx in range(start, end):
                goods_no = goods_no_list[idx]
                logging.info(
                    f"[{idx + 1}위] 상세정보 크롤링 시작 - goodsNo: {goods_no}"
                )
                try:
                    detail = get_rank_detail_info(sb, goods_no)
                    detail_list.append(detail)
                    logging.info(f"[{idx + 1}위] 크롤링 성공 - goodsNo: {goods_no}")
                except Exception as e:
                    logging.error(
                        f"[{idx + 1}위] 크롤링 실패 - goodsNo: {goods_no} | 에러: {e}"
                    )
                    failed_goods.append(goods_no)
                    detail_list.append({})

    # 실패 수 기준 검증
    num_failed = len(failed_goods)
    fail_ratio = num_failed / total

    # 기준: 5개 이상 실패 or 실패율 10% 이상
    if num_failed >= 5 or fail_ratio >= 0.1:
        logging.error(f"❌ 크롤링 실패율 초과: 실패 {num_failed}/{total}건")
        raise AirflowFailException("❌ 크롤링 실패율 초과 → DAG 실패 처리")

    logging.info("데이터 병합 및 저장")
    df_basic = pd.DataFrame(data)
    df_detail = pd.DataFrame(detail_list)
    result_df = pd.concat(
        [df_basic.reset_index(drop=True), df_detail.reset_index(drop=True)], axis=1
    )

    # 저장 경로 지정
    dag_id = context["dag"].dag_id
    if dag_id == "crawl_suncare_afternoon_dag":
        time_str = "170100"
    elif dag_id == "crawl_suncare_morning_dag":
        time_str = "093000"
    else:
        time_str = "000000"

    ts = datetime.now().strftime("%Y%m%d")
    filename = f"rank_suncare_result_{ts}_{time_str}.json"
    local_path = f"/opt/airflow/data/suncare/{filename}"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    result_df.to_json(local_path, orient="records", force_ascii=False, indent=2)
    logging.info(f"✅ 저장 파일명: {local_path}")

    context["ti"].xcom_push(key="local_path", value=local_path)
    context["ti"].xcom_push(key="s3_key", value=f"raw_data/non_pb/suncare/{filename}")
    logging.info(f"저장 완료: {local_path}")


def upload_to_s3(**context):
    local_path = context["ti"].xcom_pull(task_ids="crawl_suncare", key="local_path")
    s3_key = context["ti"].xcom_pull(task_ids="crawl_suncare", key="s3_key")
    bucket_name = "de6-team5-bucket"

    try:
        hook = S3Hook(aws_conn_id="de6-team5-bucket")
        hook.load_file(
            filename=local_path, key=s3_key, bucket_name=bucket_name, replace=True
        )
        logging.info(f"S3 업로드 성공: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.warning(f"S3 업로드 실패: {e}")
        raise


# =======  DAG 정의 =======
local_tz = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack.on_failure_callback,
    "on_retry_callback": slack.on_retry_callback,
}

with DAG(
    dag_id="crawl_suncare_morning_dag",
    default_args=default_args,
    # schedule_interval = "30 9 * * *",  Airflow 2버전
    schedule="30 9 * * *",
    start_date=datetime(2025, 7, 1, tzinfo=local_tz),
    catchup=False,
) as dag_morning:
    crawl_suncare = PythonOperator(
        task_id="crawl_suncare",
        python_callable=crawl_suncare_data,
    )

    upload_json_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    crawl_suncare >> upload_json_to_s3

with DAG(
    dag_id="crawl_suncare_afternoon_dag",
    default_args=default_args,
    # schedule_interval = "1 17 * * *", Airflow 2버전
    schedule="1 17 * * *",
    start_date=datetime(2025, 7, 1, tzinfo=local_tz),
    catchup=False,
) as dag:
    crawl_suncare = PythonOperator(
        task_id="crawl_suncare",
        python_callable=crawl_suncare_data,
    )

    upload_json_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    crawl_suncare >> upload_json_to_s3
