import pandas as pd
from sqlalchemy import create_engine


def ingest_callable(user,host,password,port,db,table_name,parquet_file):
    data_file_name = parquet_file
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()
    print("Connection Established , Inserting data ...................")
    df = pd.read_parquet(data_file_name, engine='auto')
    print(pd.io.sql.get_schema(df.reset_index(), name=table_name, con=engine))

    df.to_sql(name=table_name, con=engine, chunksize=100000, if_exists='replace')
    print(df.head())
    print("Data Inserted Successfully !!")


