#!/usr/bin/env bash

# airflow db init
airflow db upgrade

# set airflow connections
printf "Setting airflow connections...\n\n"


# GCP conection
airflow connections delete google_cloud_platform
airflow connections add 'google_cloud_platform' \
    --conn-json '{
        "conn_type": "google-cloud-platform",
        "extra": {
            "extra__google_cloud_platform__key_path": "/opt/airflow/keys/gcp_keys.json"
        }
    }'

# Monitoring
airflow connections delete discord_webhook
airflow connections add 'discord_webhook' \
    --conn-json '{
        "conn_type": "http",
        "password": "webhoook_server_url"
    }'


# Create user
airflow users create -r Admin \
    -u airflow_metadata \
    -f airflow \
    -l admin \
    -p password_airflow \
    -e samsudiney@gmail.com

airflow scheduler &
airflow webserver &
airflow triggerer