FROM apache/airflow:2.6.0-python3.9
USER root

# creating working folder for pipeline
RUN mkdir -p usage_report/output
RUN mkdir -p /opt/airflow/dags
RUN mkdir -p /opt/airflow/scripts
RUN mkdir -p /opt/airflow/logs
RUN mkdir -p /opt/airflow/dbt_venv
RUN mkdir -p data_quality/output

# gave permission to folder
RUN chmod -R 777 /opt/airflow/logs
RUN chmod -R 777 data_quality/output

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


USER airflow

ARG AIRFLOW_HOME=/opt/airflow

# installing python library from pypi
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org \
  apache-airflow-providers-google==8.4.0 \
  jsonschema==4.17.0 \
  mysql-connector==2.2.9 \
  numpy==1.22.4 \
  pandas==1.5.1 \
  pyarrow==6.0.1 \
  pytz==2022.6 \
  jsonrpcclient==4.0.2 \
  google-play-scraper==1.2.2 \
  app-store-scraper==0.3.5 \
  itunes-app-scraper-dmi==0.9.5 \
  psycopg2-binary==2.9.5 \
  pandas-gbq==0.17.9 \
  xlsxwriter==3.0.3 \
  mailchimp-marketing==3.0.80 \
  nameparser==1.1.2 \
  pympler==1.0.1 \
  astronomer-cosmos==0.7.5 \
  authlib
#  \
# acryl-datahub-airflow-plugin

# Copy source code
COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

# expose port
EXPOSE 8080 5555 8793

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

