from tqdm.auto import tqdm
import pandas as pd
from sqlalchemy import create_engine
import click

# pg_user = "root"
# pg_pass = "password"
# pg_host = 'localhost'
# pg_db = "ny_taxi"
# pg_port = 5432
# target_table = 'yellow_taxi_data'


@click.command()
@click.option('--pg-user', envvar='PG_USER', required=True, help='Postgres user')
@click.option('--pg-pass', 'pg_pass', envvar='PG_PASS', required=True, help='Postgres password')
@click.option('--pg-host', envvar='PG_HOST', default='localhost', show_default=True, help='Postgres host')
@click.option('--pg-port', envvar='PG_PORT', default=5432, type=int, show_default=True, help='Postgres port')
@click.option('--pg-db', 'pg_db', envvar='PG_DB', required=True, help='Postgres database')
@click.option('--target-table', 'target_table', envvar='TARGET_TABLE', default='yellow_taxi_data', show_default=True, help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = 'yellow_tripdata_2021-01.csv.gz'

    dtype = {
        "VendorID": "Int64",
        "passenger_count": "Int64",
        "trip_distance": "float64",
        "RatecodeID": "Int64",
        "store_and_fwd_flag": "string",
        "PULocationID": "Int64",
        "DOLocationID": "Int64",
        "payment_type": "Int64",
        "fare_amount": "float64",
        "extra": "float64",
        "mta_tax": "float64",
        "tip_amount": "float64",
        "tolls_amount": "float64",
        "improvement_surcharge": "float64",
        "total_amount": "float64",
        "congestion_surcharge": "float64"
    }

    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

    # read the whole CSV file at once
    df = pd.read_csv(prefix+url, dtype=dtype, parse_dates=parse_dates)

    # get the create table syntax - view purpose only - that will run implicitly when using df.to_sql()
    print(pd.io.sql.get_schema(df, name=target_table, con=engine))

    # this will basically create a table without any data
    df.head(0).to_sql(con=engine, name=target_table, if_exists='replace')

    df_iter = pd.read_csv(prefix+url, dtype=dtype,
                          parse_dates=parse_dates, iterator=True, chunksize=100000)

    for df in tqdm(df_iter):
        df.to_sql(con=engine, name=target_table, if_exists='append')


if __name__ == '__main__':
    run()
