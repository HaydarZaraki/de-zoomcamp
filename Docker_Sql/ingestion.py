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
    url = params.url
    file_name = 'output.parquet'
    os.system(f'wget -O output.parquet {url}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df = pd.read_parquet(file_name, engine='auto')

    pd.io.sql.get_schema(df.reset_index(), name=table_name, con=engine)

    df.to_sql(name=table_name, con=engine, chunksize=100000, if_exists='replace')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingestion Parameters')
    parser.add_argument('--user', help='user of postgres')
    parser.add_argument('--host', help='host of postgres')
    parser.add_argument('--port', help='port of postgres')
    parser.add_argument('--password', help='password of postgres')
    parser.add_argument('--db', help='db of postgres')
    parser.add_argument('--table_name', help='table name')
    parser.add_argument('--url', help='URL of CSV file')
    args = parser.parse_args()
    main(args)
