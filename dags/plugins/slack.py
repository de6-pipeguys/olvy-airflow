from collections import deque
from airflow.models import Variable
import requests
import os
from datetime import timedelta

def tail(file_path, n=10):
    try:
        with open(file_path, 'r') as f:
            return list(deque(f, n))
    except Exception as e:
        return [f"Log file not found: {e}"]

def get_log_path(ti):
    """
    Airflow 로그 경로를 생성합니다.
    """
    airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
    dag_id = ti.dag_id
    run_id = ti.run_id
    task_id = ti.task_id
    try_number = ti.try_number
    # Airflow 로그 파일명 규칙에 맞게 경로 생성
    log_path = os.path.join(
        airflow_home,
        "logs",
        f"dag_id={dag_id}",
        f"run_id={run_id}",
        f"task_id={task_id}",
        f"attempt={str(int(try_number) - 1)}.log"
    )
    return log_path

def on_failure_callback(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id if ti else ''
    task_id = ti.task_id if ti else ''
    state = ti.state if ti else ''
    # KST 변환 (UTC + 9시간)
    if ti and ti.start_date:
        start_date_kst = (ti.start_date + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
    else:
        start_date_kst = ''

    # 로그 마지막 20줄 추출
    log_path = get_log_path(ti)
    log_tail = '\n'.join(tail(log_path, 20))

    message = (
        f"*🚨 TASK 실행에 실패했습니다. 태스크를 확인해주세요.*\n"
        f"\n"
        f"- dag_id : {dag_id}\n"
        f"- task_id : {task_id}\n"
        f"- 시작 시간: {start_date_kst} (KST)\n"
        f"- 상태 : {state}\n"
        f"- Airflow log :\n``` {log_tail} ```"
    )
    send_message_to_a_slack_channel(message, ":scream:")

def on_retry_callback(context):
    ti = context.get('task_instance')
    dag_id = ti.dag_id if ti else ''
    task_id = ti.task_id if ti else ''
    state = ti.state if ti else ''
    start_date = (ti.start_date).strftime('%Y-%m-%d %H:%M:%S')
    # KST 변환 (UTC + 9시간)
    # if ti and ti.start_date:
    #     start_date_kst = (ti.start_date + timedelta(hours=9)).strftime('%Y-%m-%d %H:%M:%S')
    # else:
    #     start_date_kst = ''

    # 로그 마지막 20줄 추출
    log_path = get_log_path(ti)
    log_tail = '\n'.join(tail(log_path, 20))

    message = (
        f"*⚠️ task 실행에 실패하여 5분 뒤에 다시 시도합니다.*\n"
        f"\n"
        f"- dag_id : {dag_id}\n"
        f"- task_id : {task_id}\n"
        f"- task 시작 시간: {start_date} \n"
        f"- 상태 : {state}\n"
        f"- Airflow log :\n``` {log_tail} ```"
    )
    send_message_to_a_slack_channel(message, ":scream:")

def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    headers = {'content-type': 'application/json'}
    data = { "username": "Data GOD", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r