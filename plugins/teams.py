from airflow.models import Variable
import requests
import pendulum
import time

# Get your local time zone
local_tz = pendulum.timezone(Variable.get("AIRFLOW_TZ"))

def get_local_tz(date):
        
    return date.astimezone(local_tz) 

def on_failure_callback(context):
    """
    :return: operator.execute
    """
    # Convert execution date to local time zone
    logical_date_local = get_local_tz(context.get('logical_date'))
    task_instance = context['task_instance']
    task_id = task_instance.task_id 
    
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "Summary",
        "sections": [{
        "activityTitle": "\U0001F622 Task Failed",
        "facts": [
            {
            "name": "*Dag*",
            "value": task_instance.dag_id
            },
            {
            "name": "*Task*",
            "value": task_id
            },
            {
            "name": "*Execution Time*",
            "value": logical_date_local.strftime("%Y-%m-%d %H:%M:%S")
            },
            {
            "name": "*Exception*",
            "value": repr(context.get('exception')),
            },
            {
            "name": "*Log Url*",
            "value": task_instance.log_url
            }, 
            ],
        }],
    }      

    send_message_to_a_teams_channel(payload)

def on_success_callback(context):
    """
    :return: operator.execute
    """
    time.sleep(10) #xcom 비동기 처리된 값 받아오기 위한 지연
    
    # Convert execution date to local time zone
    logical_date_local = get_local_tz(context.get('logical_date'))
    task_instance = context['task_instance']
    task_id = task_instance.task_id

    xcom_msg = ""
    xcom_msg = task_instance.xcom_pull(key='teams_msg')
    
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": "Summary",
        "sections": [{
        "activityTitle": "\U0001F60A Task Successed",
        "facts": [
            {
            "name": "*Dag*",
            "value": task_instance.dag_id
            },
            {
            "name": "*Task*",
            "value": task_id
            },
            {
            "name": "*Execution Time*",
            "value": logical_date_local.strftime("%Y-%m-%d %H:%M:%S")
            },
            {
            "name": "*Log Url*",
            "value": task_instance.log_url
            },
            {
            "name": "*Detail Message*",
            "value": xcom_msg
            },
            ],
        }],
    } 

    send_message_to_a_teams_channel(payload)


# def send_message_to_a_teams_channel(message, emoji, channel, access_token):
def send_message_to_a_teams_channel(message):
    url = Variable.get("TEAMS_WEBHOOK_URL")

    headers = {
        'content-type': 'application/json',
    }
    r = requests.post(url, json=message, headers=headers)

    return r

