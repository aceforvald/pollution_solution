import os
import configparser
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

# Get config details from config.ini (for use by postgres_creator() and _start_post())
def get_postgres_config():
    try:
        print('Hello my name is get_postgres_config()')
        CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
        config = configparser.ConfigParser()
        config.read(CURR_DIR_PATH + "/config.ini")

        # Fetches the api key from your config.ini file
        dbname = config.get('postgres', 'dbname')
        user = config.get('postgres', 'user')
        pw = config.get("postgres", "pw")
        host = config.get('postgres', 'host')
        port = config.get('postgres', 'port')

        print(user)
        print(f'and the password is {pw}')

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
    print('Hello my name is _start_post()')
    print('calling get_postgres_config()')
    dbname, user, pw, host, port = get_postgres_config()
    print(f'I got some cool variables, like user is {user}, and dbname is {dbname}')

    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

    # Create postgres engine
    postgres_engine = create_engine(
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}",
        creator=postgres_creator
    )

    # Create Pandas dataframes from cleansed data csvs
    sensors_data = pd.read_csv(
        CURR_DIR_PATH + "/sensors.csv",
        sep=",",
    )

    # Create postgres tables according to data source
    sensors_data.to_sql(name="sensors_data", con=postgres_engine, if_exists="replace", index=False)

#print('calling _start_post()')
#_start_post()

print('testing connection')
test_connection()