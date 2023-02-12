import pandas as pd
from sqlalchemy import create_engine
from time import time

def load_data_iterator():
    data_iter = pd.read_csv('yellow_tripdata_2022-01.csv', iterator=True, chunksize=100000)
    return data_iter

def load_data_chunk(data):
    data.drop(columns='Unnamed: 0', inplace=True)
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)
    return data

def create_empty_table(data_iter, engine):
    data = next(data_iter)
    data.drop(columns='Unnamed: 0', axis=0, inplace=True)
    data.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')
    ingest_to_db(data, engine)

def ingest_to_db(data, engine):
    data.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

def main():
    engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()
    data_iter = load_data_iterator()

    create_empty_table(data_iter, engine)

    for data in data_iter:
        t_start = time()
        
        data = load_data_chunk(data)
        ingest_to_db(data, engine)

        t_end = time()

        print(f'inserted a batch of data, took {t_end - t_start} seconds')


if __name__ == "__main__":
    main()
