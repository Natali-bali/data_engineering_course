import pandas as pd
from sqlalchemy import create_engine
import time

def ingest_callable(user, password, host, port, db, table_name, csv_name):
    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print("Connection established...")
    #Create small (itarable)  df
    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 50000)
    df = next(df_iter)

    # Create table, first transfer text data to datetime
    t_start = time.time()
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(0).to_sql(name = table_name, con = engine, if_exists = 'replace')
    df.to_sql(name = table_name, con = engine, if_exists = 'append')
    t_finish = time.time()
    print(f'First chunk added in time: {t_finish - t_start}')
    # loop to download sql

    while True:
        t_start = time.time()
        df = next(df_iter)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df.to_sql(name = table_name, con = engine, if_exists = 'append')
        t_finish = time.time()
        print(f'Another chunk added in time: {t_finish - t_start}')
