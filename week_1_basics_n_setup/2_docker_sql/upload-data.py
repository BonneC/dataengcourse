import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

from fastparquet import ParquetFile


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    filename = 'output.parquet'


    # download parquet
    print("download data")
    os.system(f"wget {url} -O {filename}")

    print("read parquet file")
    df = pd.read_parquet(url)

    print("create connection to db")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    # # Custom code for parquet
    # 1. Create an empty table (using n=0 rows) `if_exists='replace'`
    print("create table")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    print("insert rows")
    # 2. Insert the chunks `if_exists='append'`
    pf = ParquetFile('yellow_tripdata_2021-01.parquet')
    for df in pf.iter_row_groups():
        t_start = time()
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the parquet file')

    args = parser.parse_args()

    main(args)

# get_ipython().system('wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
# df_zones = pd.read_csv('taxi+_zone_lookup.csv')
# df_zones.head()
# df_zones.to_sql(name='zones', con=engine, if_exists='replace')
