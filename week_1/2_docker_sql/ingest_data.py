import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = 'output.csv'

    # Download csv
    os.system(f"wget {url} -O {csv_name}")

    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #Create small (itarable)  df
    df_iter = pd.read_csv(csv_name, iterator = True, chunksize = 50000)
    df = next(df_iter)

    # Create table, first transfer text data to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df.head(0).to_sql(name = table_name, con = engine, if_exists = 'replace')
    df.to_sql(name = table_name, con = engine, if_exists = 'append')

    # loop to download sql
    while True:
        t_start = time.time()
        df = next(df_iter)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df.to_sql(name = table_name, con = engine, if_exists = 'append')
        t_finish = time.time()
        print(f'Another chunk added in time: {t_finish - t_start}')

if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, db (database name), table_name, url (of the csv)

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)

