import os
import configparser
import pandas as pd
import psycopg2 as ps
from sqlalchemy import create_engine

# Creates a connection to postgres server
def postgres_creator():
    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    config = configparser.ConfigParser()
    config.read(CURR_DIR_PATH + "/config.ini")

    # Fetches the api key from your config.ini file
    PW = config.get("DEV", "pw")
    
    return ps.connect(
            dbname="enni_sini",
            user="enni_sini",
            password=PW,
            host="localhost",
            port="5432"
    )

def _start_post():

    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

    # Create postgres engine
    postgres_engine = create_engine(
        url="postgresql+psycopg2://localhost",
        creator=postgres_creator
    )

    # Create Pandas dataframes from cleansed data csvs
    smhi_data = pd.read_csv(
        CURR_DIR_PATH + "/smhi_cleaned.csv",
        sep=",",
    )

    fmi_data = pd.read_csv(
        CURR_DIR_PATH + "/fmi_cleaned.csv",
        sep=",",
    )

    # Create two postgres tables according to data source
    smhi_data.to_sql(name="smhi_data", con=postgres_engine, if_exists="replace", index=False)
    fmi_data.to_sql(name="fmi_data", con=postgres_engine, if_exists="replace", index=False)