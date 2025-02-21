FROM apache/airflow:2.6.0-python3.9

USER root

# Creating working folders for pipeline
RUN mkdir -p usage_report/output
RUN mkdir -p /opt/airflow/dags
RUN mkdir -p /opt/airflow/scripts
RUN mkdir -p /opt/airflow/logs
RUN mkdir -p /opt/airflow/dbt_venv
RUN mkdir -p data_quality/output

# Give permission to folders
RUN chmod -R 777 /opt/airflow/logs
RUN chmod -R 777 data_quality/output

# Install necessary packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  vim \
  wget \
  unzip \
  libnss3 \
  libgconf-2-4 \
  libxss1 \
  libappindicator3-1 \
  libasound2 \
  fonts-liberation \
  libatk-bridge2.0-0 \
  libgtk-3-0 \
  xdg-utils \
  gnupg2 \
  && curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update \
  && apt-get install -y google-chrome-stable \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Switch to airflow user to install Python packages
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    webdriver-manager \
    selenium \
    beautifulsoup4 \
    pandas \
    google-cloud-bigquery

# Set AIRFLOW_HOME
ARG AIRFLOW_HOME=/opt/airflow

# Installing additional Python libraries from PyPI
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

# Copy source code
COPY ./dags /opt/airflow/dags
COPY ./scripts /opt/airflow/scripts

# Expose ports
EXPOSE 8080 5555 8793

# Copy requirements.txt
COPY requirements.txt /requirements.txt

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
