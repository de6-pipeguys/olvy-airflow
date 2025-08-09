from collections import deque
from airflow.sdk import Variable
import requests
import os
from datetime import timedelta
import time

# ✅ DAG별 담당자 매핑 (필요 시 수정)
TEAM_OWNERS = {
    # 예: 'my_dag_id': 'charlie'

    ## 랭킹
    'rank_crawl_morning_skincare': 'jun',
    'rank_crawl_afternoon_skincare': 'jun',

    'rank_crawl_morning_food': 'jun',
    'rank_crawl_afternoon_food': 'jun',

    'rank_crawl_morning_cleansing': 'jun',
    'rank_crawl_afternoon_cleansing': 'jun',

    'rank_crawl_morning_suncare': 'jun',
    'rank_crawl_afternoon_suncare': 'jun',

    'rank_crawl_morning_manscare': 'jun',
    'rank_crawl_afternoon_manscare': 'jun',

    'rank_crawl_morning_haircare': 'jun',
    'rank_crawl_afternoon_haircare': 'jun',

    'rank_crawl_morning_healthcare': 'jun',
    'rank_crawl_afternoon_healthcare': 'jun',

    ## PB 브랜드
    'pb_brand_crawl_skincare': 'jun',
    'pb_brand_crawl_food': 'jun',
    'pb_brand_crawl_suncare': 'jun',
    'pb_brand_crawl_manscare': 'jun',
    'pb_brand_crawl_healthcare': 'jun'
}

def tail(file_path, n=20):
    try:
        with open(file_path, 'r') as f:
            return list(deque(f, n))
    except Exception as e:
        return [f"Log file not found: {e}"]

def get_log_path(ti):
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    dag_id = getattr(ti, 'dag_id', 'unknown')
    run_id = getattr(ti, 'run_id', 'unknown')
    task_id = getattr(ti, 'task_id', 'unknown')
    try_number = getattr(ti, 'try_number', 1)

    log_path = os.path.join(
        airflow_home,
        "logs",
        f"dag_id={dag_id}",
        f"run_id={run_id}",
        f"task_id={task_id}",
        f"attempt={str(int(try_number))}.log"
    )
    return log_path

def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    headers = {'content-type': 'application/json'}
    data = {"username": "Data GOD", "text": message, "icon_emoji": emoji}
    r = requests.post(url, json=data, headers=headers)
    return r

def slack_callback(context, mode="failure"):
    ti = context.get('task_instance')
    dag_id = getattr(ti, 'dag_id', 'unknown')
    task_id = getattr(ti, 'task_id', 'unknown')
    state = getattr(ti, 'state', 'unknown')

    # ✅ TEAM_OWNERS dict에서 user_id 매핑
    user_id = TEAM_OWNERS.get(dag_id, 'unknown')

    # KST 기준 시작 시간 (있으면)
    if ti and getattr(ti, 'start_date', None):
        start_date = (ti.start_date + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        start_date = 'unknown'

    log_path = get_log_path(ti)
    time.sleep(2)  # 로그 기록 대기
    log_tail = '\n'.join(tail(log_path, 20))

    if mode == "failure":
        title = "*🚨 TASK 실행에 실패했습니다. 태스크를 확인해주세요.*"
    elif mode == "retry":
        title = "*⚠️ task 실행에 실패하여 5분 뒤에 다시 시도합니다.*"
    else:
        title = "*알림*"

    message = (
        f"{title}\n"
        f"\n"
        f"- user_id : {user_id}\n"
        f"- dag_id : {dag_id}\n"
        f"- task_id : {task_id}\n"
        f"- 시작 시간: {start_date}\n"
        f"- 상태 : {state}\n"
        f"- Airflow log :\n```{log_tail}```"
    )
    send_message_to_a_slack_channel(message, ":scream:")

def on_failure_callback(context):
    slack_callback(context, mode="failure")

def on_retry_callback(context):
    slack_callback(context, mode="retry")

