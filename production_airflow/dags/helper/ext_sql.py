import os
import logging
import pytz
import time
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

POSTGRES_HOST = os.getenv("host")
POSTGRES_PORT = os.getenv("port")
POSTGRES_USER = os.getenv("user")
POSTGRES_PASSWORD = os.getenv("password")
POSTGRES_DATABASE = os.getenv("dbname")

def get_yesterday_date():
    return (datetime.now(pytz.timezone("Asia/Jakarta")) - timedelta(days=1)).strftime('%Y-%m-%d')

def data_info(dataframe, table_name):
    """Logs DataFrame details for debugging."""
    logging.info(f"Table: {table_name}")
    logging.info(f"Dataframe Info = {dataframe.info()}")
    logging.info(f"Data Types = {dataframe.dtypes}")
    logging.info(f"Number of Rows = {len(dataframe)}")
    logging.info(f"Head of Dataframe = \n{dataframe.head()}")

def extract_table(table, date_filter=None):
    """Extracts a single table from PostgreSQL with optional date filter."""
    try:
        # create connection to PostgreSQL
        engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE}')
        sql_query = f"SELECT * FROM library.{table}"
        if date_filter:
            sql_query += f" WHERE created_at >= '{date_filter} 00:00:00' AND created_at < '{date_filter} 23:59:59'"
        df = pd.read_sql_query(sql_query, engine)
        if df.empty:
            logging.warning(f"No data extracted from {table}. Skipping...")
            return None
        data_info(df, table.split('.')[-1])
        return df
    except Exception as e:
        logging.error(f"Error extracting {table}: {e}")
        raise

def extract_table_data(table):
    """Extracts a single table into a DataFrame."""
    retries = 3
    date_filter = get_yesterday_date() 
    for attempt in range(retries):
        try:
            logging.info(f"Attempting to extract {table} (Attempt {attempt + 1}/{retries})")
            df = extract_table(table, date_filter)  
            if df is not None:
                logging.info(f"Extracted {table} with shape: {df.shape}")
                return df
            break
        except Exception as e:
            logging.error(f"Error extracting {table} (Attempt {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise
