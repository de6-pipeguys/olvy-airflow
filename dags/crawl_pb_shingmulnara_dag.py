from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from seleniumbase import SB
import logging
from crawler.crawl_brand_suncare import get_brand, get_pbbrand_detail_info
from plugins import slack
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum
from airflow.exceptions import AirflowFailException

##### 실행 코드 #####
PB_BRAND_CODE_DICT = {
    "바이오힐 보": "A000897",
    "브링그린": "A002253",
    "웨이크메이크": "A001240",
    "컬러그램": "A002712",
    "필리밀리": "A002502",
    "아이디얼포맨": "A001643",
    "라운드어라운드": "A001306",
    "식물나라": "A000036",
    "케어플러스": "A003339",
    "탄탄": "A015673",
    "딜라이트 프로젝트": "A003361",
}


def crawl_shingmulnara_with_detail(**context):
    brand_code = "A000036"  # 식물나라
    df = get_brand(brand_code)

    logging.info(f"✅ 수집된 제품 수: {len(df)}개")

    if df.empty:
        logging.warning("❌ 크롤링 결과가 비었습니다")
        return

    with SB(uc=True, test=True, headless=True) as sb:
        detail_list = []
        failed_goods = []
        for idx, row in df.iterrows():
            goods_no = row["goodsNo"]
            # product_name = row.get('goodsName', '이름없음')

            logging.info(
                f"[{idx + 1}/{len(df)}] 상세정보 크롤링 시작 - goodsNo: {goods_no}"
            )
            try:
                detail = get_pbbrand_detail_info(sb, goods_no)
                detail_list.append(detail)
                logging.info(f"[{idx + 1}/{len(df)}] 크롤링 성공 - goodsNo: {goods_no}")
            except Exception as e:
                logging.warning(
                    f"'[{idx + 1}/{len(df)}] 크롤링 실패 - goodsNo: {goods_no} | 에러: {e}"
                )
                failed_goods.append(goods_no)
                detail_list.append({})  # 실패해도 빈 값이라도 넣어주기

        # ❗ 실패율 검증
        total = len(df)
        num_failed = len(failed_goods)
        fail_ratio = num_failed / total

        if fail_ratio >= 0.1 or num_failed >= 5:
            logging.error(f"❌ 크롤링 실패율 초과 - 실패 상품 수: {num_failed}/{total}")
            raise AirflowFailException("❌ 크롤링 실패율 초과 → DAG 실패 처리")

    # 최종 저장 시 컬럼에서 goodsNo 제거
    df.drop(columns=["goodsNo"], inplace=True)
    detail_df = pd.DataFrame(detail_list)
    result_df = pd.concat(
        [df.reset_index(drop=True), detail_df.reset_index(drop=True)], axis=1
    )

    # 저장 경로 지정
    ts = datetime.now().strftime("%Y%m%d")
    filename = f"pb_shingmulnara_result_{ts}_130000.json"
    local_path = f"/opt/airflow/data/shingmulnara/{filename}"
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    result_df.to_json(local_path, orient="records", force_ascii=False, indent=2)
    logging.info(f"✅ 저장 파일명: {local_path}")

    context["ti"].xcom_push(key="local_path", value=local_path)
    context["ti"].xcom_push(key="s3_key", value=f"raw_data/pb/shingmulnara/{filename}")
    print(f"저장 완료: {local_path}")


def upload_to_s3(**context):
    local_path = context["ti"].xcom_pull(
        task_ids="crawl_pbbrand_shingmulnara", key="local_path"
    )
    s3_key = context["ti"].xcom_pull(
        task_ids="crawl_pbbrand_shingmulnara", key="s3_key"
    )
    bucket_name = "de6-team5-bucket"  # 실제 버킷명으로 바꿔야 함

    try:
        hook = S3Hook(aws_conn_id="de6-team5-bucket")
        hook.load_file(
            filename=local_path, key=s3_key, bucket_name=bucket_name, replace=True
        )
        logging.info(f"S3 업로드 성공: s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"S3 업로드 실패: {e}")
        raise


# DAG 정의
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
    dag_id="shingmulnara_crawling_dag",
    default_args=default_args,
    # schedule_interval="0 13 * * *",  Airflow 2버전
    schedule="0 13 * * *",
    start_date=datetime(2025, 7, 1, tzinfo=local_tz),
    catchup=False,
) as dag:
    task_crawl = PythonOperator(
        task_id="crawl_pbbrand_shingmulnara",
        python_callable=crawl_shingmulnara_with_detail,
    )

    task_upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    task_crawl >> task_upload
