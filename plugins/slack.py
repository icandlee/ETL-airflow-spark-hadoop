from airflow.models import Variable

import logging
import requests

def on_failure_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    message = """
            :red_circle: Task Failed.
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}
            *Exception*: {exception}
            *Log Url*: {log_url}
            """.format(
        dag=context.get('task_instance').dag_id,
        task=context.get('task_instance').task_id,
        exec_date=context.get('logical_date'),
        exception=context.get('exception'),
        log_url=context.get('task_instance').log_url
    )
    emoji = ":crying_cat_face:"

    send_message_to_a_slack_channel(message, emoji)

def on_success_callback(context):
    """
    https://airflow.apache.org/_modules/airflow/operators/slack_operator.html
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    message = """
            :smile_cat: Task Successed.
            *Dag*: {dag}
            *Task*: {task}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
        dag=context.get('task_instance').dag_id,
        task=context.get('task_instance').task_id,
        exec_date=context.get('logical_date'),
        exception=context.get('exception'),
        log_url=context.get('task_instance').log_url
    )
    emoji = ":crying_cat_face:"

    send_message_to_a_slack_channel(message, emoji)


# def send_message_to_a_slack_channel(message, emoji, channel, access_token):
def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = { "username": "Airflow", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r

