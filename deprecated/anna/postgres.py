import os
import configparser
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

# Get config details from config.ini (for use by postgres_creator() and _start_post())
def get_postgres_config():
    try:
        CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
        config = configparser.ConfigParser()
        config.read(CURR_DIR_PATH + "/config.ini")

        # Fetches the api key from your config.ini file
        dbname = config.get('postgres', 'dbname')
        user = config.get('postgres', 'user')
        pw = config.get("postgres", "pw")
        host = config.get('postgres', 'host')
        port = config.get('postgres', 'port')

        return dbname, user, pw, host, port
    except Exception as e:
        print(f'get_postgres_config() isn\'t happy with you: {e}')
    
# Creates a connection to postgres server
def postgres_creator():
    try:
        dbname, user, pw, host, port = get_postgres_config()
        return ps.connect(
                dbname = dbname,
                user = user,
                password = pw,
                host = host,
                port = port
        )
    
    except Exception as e:
        print(f'postgres_creator() isn\'t happy with you: {e}')

def test_connection():
    try:
        dbname, user, pw, host, port = get_postgres_config()
        return ps.connect(
                dbname=dbname,
                user=user,
                password=pw,
                host=host,
                port=port
        )
    
    except Exception as e:
        print(f'Test connection failed: {e}')

def _start_post():
    dbname, user, pw, host, port = get_postgres_config()
    print(f'Posting to the database {dbname} as {user}')

    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

    # Create postgres engine
    postgres_engine = create_engine(
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}",
        creator=postgres_creator
    )

    # Create Pandas dataframes from cleansed data csvs
    dim_city = pd.read_csv(
        CURR_DIR_PATH + "/dim_city.csv",
        sep=",",
    )

    dim_country = pd.read_csv(
        CURR_DIR_PATH + "/dim_country.csv",
        sep=",",
    )

    dim_location = pd.read_csv(
        CURR_DIR_PATH + "/dim_location.csv",
        sep=",",
    )

    dim_time = pd.read_csv(
        CURR_DIR_PATH + "/dim_time.csv",
        sep=",",
    )

    dim_values = pd.read_csv(
        CURR_DIR_PATH + "/dim_values.csv",
        sep=",",
    )

    relations = pd.read_csv(
        CURR_DIR_PATH + "/relations.csv",
        sep=",",
    )

    # Create postgres tables according to data source
    dim_city.to_sql(name="dim_city", con=postgres_engine, if_exists="replace", index=False)
    dim_country.to_sql(name="dim_country", con=postgres_engine, if_exists="replace", index=False)
    dim_location.to_sql(name="dim_location", con=postgres_engine, if_exists="replace", index=False)
    dim_time.to_sql(name="dim_time", con=postgres_engine, if_exists="replace", index=False)
    dim_values.to_sql(name="dim_values", con=postgres_engine, if_exists="replace", index=False)
    relations.to_sql(name="relations", con=postgres_engine, if_exists="replace", index=False)

print('calling _start_post()')
_start_post()

#print('testing connection')
#test_connection()