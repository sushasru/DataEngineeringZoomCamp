import os
import argparse
import pandas as pd

from time import time
from sqlalchemy import create_engine

def ingest_data(tablename, url, csvname, user, password,host, port, db):
    print("\n*************************\n\tDetails for current table: \n\t\thost={}\n\t\tport={}\n\t\tdb={}\n\t\tcsv-url={}\n\t\ttablename-{}\n*************************\n".format(host,port,db,url,tablename))
    os.system(f"wget {url} -O {csvname}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    df_iter = pd.read_csv(csvname, iterator=True, chunksize=100000)

    df= next(df_iter)

    if csvname=='yellow_output.csv':
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    elif csvname=='green_output.csv':
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tablename, con=engine, if_exists='replace')
    df.to_sql(name=tablename, con=engine, if_exists='append')

    while True:
        try:
            t_start=time()
            df=next(df_iter)

            if csvname=='yellow_output.csv':
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            elif csvname=='green_output.csv':
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=tablename, con=engine, if_exists='append')
            t_end=time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
        except StopIteration:
            print('completed')
            break
            

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    
    yellow_tablename=params.yellow_tablename
    yellow_url=params.yellow_url
    yellow_csvname='yellow_output.csv'

    green_tablename=params.green_tablename
    green_url=params.green_url
    green_csvname='green_output.csv'

    zone_tablename=params.zone_tablename
    zone_url=params.zone_url
    zone_csvname='zone_output.csv'
    
    ingest_data(green_tablename,green_url,green_csvname,user,password,host,port,db)
    ingest_data(zone_tablename,zone_url,zone_csvname,user,password,host,port,db)
    ingest_data(yellow_tablename,yellow_url,yellow_csvname,user,password,host,port,db)


if __name__ == '__main__':
    # Parse the command line arguments and calls the main program
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--yellow_tablename', help='name of the table where we will write the results to')
    parser.add_argument('--yellow_url', help='url of the csv file')
    parser.add_argument('--green_tablename', help='name of the table where we will write the results to')
    parser.add_argument('--green_url', help='url of the csv file')
    parser.add_argument('--zone_tablename', help='name of the table where we will write the results to')
    parser.add_argument('--zone_url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
