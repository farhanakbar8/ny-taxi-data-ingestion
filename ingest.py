import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from time import time
import argparse
import os

def load_data_iterator(csv_name):
    data_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    return data_iter

def load_data_chunk(data):
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    return data

def create_empty_table(data_iter, engine, table_name):
    data = next(data_iter)
    data.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

def ingest_to_db(data, engine, table_name):
    data.to_sql(name=table_name, con=engine, if_exists='append')

def main(params):
    user = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    filename = 'output.parquet'
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {filename}")
    data = pd.read_parquet(filename)
    data.to_csv(csv_name, index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    data_iter = load_data_iterator(csv_name)

    if inspect(engine).get_table_names():
        create_empty_table(data_iter, engine, table_name)

    for data in data_iter:
        t_start = time()
        
        data = load_data_chunk(data)
        ingest_to_db(data, engine, table_name)

        t_end = time()

        print(f'inserted a batch of data, took {t_end - t_start} seconds')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--username', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db for postgres')
    parser.add_argument('--table_name', help='table_name for postgres')
    parser.add_argument('--url', help='file url')

    args = parser.parse_args()

    main(args)
