import os

from time import time

import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import psycopg2


def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):
    print("--------------------------------")
    print(user, password, host, port)
    print("--------------------------------")
    conn = psycopg2.connect(host=host, port=port, user=user, password=password)

    # Create a cursor object
    cur = conn.cursor()

    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Create the new database
    # Check if the database already exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'nytaxi'")
    exists = cur.fetchone()

    # Create the database if it doesn't exist
    if not exists:
        cur.execute("CREATE DATABASE nytaxi")
        conn.commit()
        print("Database 'nytaxi' created.")
    else:
        print("Database 'nytaxi' already exists.")

    conn.commit()

    cur.close()
    conn.close()

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    t_start = time()
    parquetFile = pq.ParquetFile(parquet_file)
    count = 1
    for indx, batch in enumerate(parquetFile.iter_batches(batch_size=10000)):
        batch_df = batch.to_pandas()
        batch_df.tpep_pickup_datetime = pd.to_datetime(batch_df.tpep_pickup_datetime)
        batch_df.tpep_dropoff_datetime = pd.to_datetime(batch_df.tpep_dropoff_datetime)

        if indx == 0:
            batch_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
            batch_df.to_sql(name=table_name, con=engine, if_exists='append') 
        else:
            batch_df.to_sql(name=table_name, con=engine, if_exists='append') 
    
    t_end = time()

    print('inserted another chunk, took %.3f second' % (t_end - t_start))
