import os

import requests

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def slack_failure_alert(context):
    """
    Gửi cảnh báo Slack khi Task FAILED.
    """

    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    run_id = context.get("run_id")
    exec_date = context.get("ts")
    log_url = task_instance.log_url.replace("localhost", "host.docker.internal")

    message = f"""
🔥 *AIRFLOW TASK FAILED!*
-------------------------
*DAG:* `{dag_id}`
*Task:* `{task_id}`
*Run ID:* `{run_id}`
*Execution Time:* `{exec_date}`
*Log:* <{log_url}|View Logs>

⚠️ Please check again!
    """

    if SLACK_WEBHOOK_URL:
        requests.post(SLACK_WEBHOOK_URL, json={"text": message})
    else:
        print("⚠️ SLACK_WEBHOOK_URL not found!")
