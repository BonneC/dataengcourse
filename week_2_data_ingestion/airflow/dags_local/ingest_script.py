import os

from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def ingest_callable(user, password, host, port, db, table_name, filename, execution_date):
    print(host, table_name, filename, execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    print("Read parquet data...")
    parquet_table = pq.read_table(filename)
    df = parquet_table.to_pandas(self_destruct=True)
    print("Data to pandas")
    print(df.head())

    print("Check if table exists and replace if needed...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print("Inserting rows...")
    # from fastparquet import ParquetFile
    # pf = ParquetFile(filename)
    # for df in pf.iter_row_groups():
    #     df.to_sql(name=table_name, con=engine, if_exists='append')
    df.to_sql(name=table_name, con=engine, if_exists='append', chunksize=1000)
    # df.to_sql(name=table_name, con=engine, if_exists='append')
    print("DONE!")

    # t_start = time()
    # df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)
    #
    # df = next(df_iter)
    #
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #
    # df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    #
    # df.to_sql(name=table_name, con=engine, if_exists='append')
    #
    # t_end = time()
    # print('inserted the first chunk, took %.3f second' % (t_end - t_start))
    #
    # while True:
    #     t_start = time()
    #
    #     try:
    #         df = next(df_iter)
    #     except StopIteration:
    #         print("completed")
    #         break
    #
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #
    #     df.to_sql(name=table_name, con=engine, if_exists='append')
    #
    #     t_end = time()
    #
    #     print('inserted another chunk, took %.3f second' % (t_end - t_start))
