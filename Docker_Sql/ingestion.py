import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    user = params.user
    host = params.host
    password = params.password
    db = params.db
    port = params.port
    table_name = params.table_name
    trip_url = params.trip_url
    lookup_url = params.lookup_url
    data_file_name = 'output.parquet'
    lookup_file_name = 'lookup.csv'
    os.system(f'wget -O output.parquet {trip_url}')
    os.system(f'wget -O lookup.csv {lookup_url}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df = pd.read_parquet(data_file_name, engine='auto')
    print(pd.io.sql.get_schema(df.reset_index(), name=table_name, con=engine))

    df.to_sql(name=table_name, con=engine, chunksize=100000, if_exists='replace')
    print(df.head())

    df_lookup = pd.read_csv(lookup_file_name)
    df_lookup.to_sql(name='lookup_zones', con=engine, chunksize=100000, if_exists='replace')
    print(df_lookup.head())

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingestion Parameters')
    parser.add_argument('--user', help='user of postgres')
    parser.add_argument('--host', help='host of postgres')
    parser.add_argument('--port', help='port of postgres')
    parser.add_argument('--password', help='password of postgres')
    parser.add_argument('--db', help='db of postgres')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--trip_url', help='URL of trips data file')
    parser.add_argument('--lookup_url', help='URL of lookup zones data file')
    args = parser.parse_args()
    main(args)
