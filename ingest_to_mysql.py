import pandas as pd
import os
from sqlalchemy import create_engine
import time
import logging
import pymysql

# ----------- Logging Setup -----------
logging.basicConfig(
    filename="logs/mysql_ingestion.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ----------- MySQL Connection Setup -----------
DB_USER = 'root'
DB_PASSWORD = '2526'
DB_HOST = 'localhost'
DB_PORT = '3306'
DB_NAME = 'Banking'  # 🔁 Change to your DB name, e.g. 'inventory'

# ----------- Auto Create Database if not exists -----------
try:
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=int(DB_PORT)
    )
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    conn.close()
    logging.info(f"Database '{DB_NAME}' checked/created successfully.")
except Exception as e:
    logging.error(f"Error creating database: {e}")
    raise

# ----------- SQLAlchemy Engine Setup -----------
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# ----------- Function to Ingest Data -----------
def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logging.info(f"Successfully ingested {table_name} into database.")

# ----------- Load CSV Files and Ingest -----------
def load_raw_data():
    start = time.time()
    logging.info("------ Starting Ingestion ------")

    for file in os.listdir('data'):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('data', file))
                df.columns = [col.strip().replace('"', '').replace(";", "")[:64] for col in df.columns]
                table_name = file[:-4]
                logging.info(f"Ingesting file: {file}")
                ingest_db(df, table_name, engine)
            except Exception as e:
                logging.error(f"Error processing file {file}: {e}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info("------ Ingestion Complete ------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")

# ----------- Run the Script -----------
if __name__ == '__main__':
    load_raw_data()
