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

def _read_file(name):
    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    return pd.read_csv(CURR_DIR_PATH + "/data/relations/" + name + ".csv", sep=",")

def _start_post():
    dbname, user, pw, host, port = get_postgres_config()
    print(f'Posting to the database {dbname} as {user}')

    # Create postgres engine
    postgres_engine = create_engine(
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}",
        creator=postgres_creator
    )

    file_names = ["relations", "dim_time", "dim_values", "dim_location", "dim_city", "dim_country"]

    # Create Pandas dataframes from cleansed data csvs
    relations = _read_file(file_names[0])
    city = _read_file(file_names[1])
    country = _read_file(file_names[2])
    location = _read_file(file_names[3])
    time = _read_file(file_names[4])
    values = _read_file(file_names[5])

    tables = [relations, city, country, location, time, values]

    # Create postgres tables according to data source and add primary keys
    for (table, file_name) in zip(tables, file_names):
        table.to_sql(name=file_name, con=postgres_engine, if_exists="replace", index=False)

        if file_name in ["relations"]:
            continue
        else:
            postgres_engine.execute("ALTER TABLE " + file_name + " ADD PRIMARY KEY (id);")

    # alter tables to add foreing keys
    line = """ALTER TABLE relations ADD FOREIGN KEY (value_id) REFERENCES dim_values (id);
            ALTER TABLE relations ADD FOREIGN KEY (time_id) REFERENCES dim_time (id);
            ALTER TABLE relations ADD FOREIGN KEY (location_id) REFERENCES dim_location (id);
            ALTER TABLE dim_location ADD FOREIGN KEY (city_id) REFERENCES dim_city (id);
            ALTER TABLE dim_city ADD FOREIGN KEY (country_id) REFERENCES dim_country (id);"""
    postgres_engine.execute(line)

print('calling _start_post()')
_start_post()

#print('testing connection')
#test_connection()