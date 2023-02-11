### Import libraries
import pandas as pd
from sqlalchemy import create_engine
from time import time

### Read 100 examples of NY Taxi Data
data = pd.read_csv('yellow_tripdata_2022-01.csv', nrows=100)
data.drop(columns='Unnamed: 0', inplace=True)
print("**100 examples of NY Taxi Data**")
print(data.head())

### Connect to Database
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()

print("**CREATE TABLE query**")
print(pd.io.sql.get_schema(data, 'yellow_taxi_data', con=engine))

### Create empty table
data.head(0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

### Create dataframe iterator to ingest the data to database
df_iter = pd.read_csv('yellow_tripdata_2022-01.csv', iterator=True, chunksize=100000)

### Append 100000 data at a time
while True:
    t_start = time()

    data = next(df_iter)
    data = data.drop(columns='Unnamed: 0')
    data.tpep_dropoff_datetime = pd.to_datetime(data.tpep_dropoff_datetime)
    data.tpep_pickup_datetime = pd.to_datetime(data.tpep_pickup_datetime)

    data.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    t_end = time()

    print(f'inserted a batch of data, took {t_end - t_start} seconds')