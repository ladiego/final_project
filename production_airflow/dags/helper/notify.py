import requests
import os
import logging

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def send_discord_notification(message):
    """Send a notification to Discord."""
    if DISCORD_WEBHOOK_URL:
        data = {"content": message}
        try:
            response = requests.post(DISCORD_WEBHOOK_URL, json=data)
            response.raise_for_status()  # Raise an error for bad responses
            logging.info("Notification sent to Discord.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to send notification to Discord: {e}")
    else:
        logging.warning("DISCORD_WEBHOOK_URL is not set. Notification not sent.")

def notify_on_success(context):
    """Send a success notification to Discord."""
    logging.info("notify_on_success called")
    message = f"DAG {context['dag'].dag_id}, the task {context['task'].task_id} has succeeded"
    send_discord_notification(message)

def notify_on_error(context):
    """Send a error notification to Discord."""
    logging.info("notify_on_error called")
    message = f"DAG {context['dag'].dag_id}, the task {context['task'].task_id} has error"
    send_discord_notification(message)

def notify_on_retry(context):
    """Send a retry notification to Discord."""
    logging.info("notify_on_retry called")
    message = f"DAG {context['dag'].dag_id}, the task {context['task'].task_id} is retrying"
    send_discord_notification(message)