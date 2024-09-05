import os
import pandas as pd
import numpy as np

# TODO city and country dimension tables 

def _start_create_relational_database():

    # create relational_data folder for new csv files
    cwd = os.path.dirname(os.path.realpath(__file__))
    save_directory = os.path.join(cwd, 'relational_data')
    os.makedirs(save_directory, exist_ok=True)

    # read data from csv file created in api.py
    data = read_file('data\sensor_data_all.csv')

    # create new csv files for relational databases tables
    create_location_dim(data, save_directory)
    create_time_dim(data, save_directory)
    create_values_dim(data, save_directory)
    create_relations(data, save_directory)

# reads and returns sensor_data_all.csv as a pandas DataFrame
def read_file(path):
    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, path)
    data = pd.read_csv(directory)
    return data

# saves DataFrame as a csv file
def save_file(file_name, df, save_directory):
    name = os.path.join(save_directory, file_name)
    df.to_csv(name, mode='w', index=False)

# creates csv file for location dimensions table
def create_location_dim(data, save_directory):
    # slice copy of original pandas data frame for location dimensions dataframe
    location_df = data[['id','location', 'lat', 'lon', 'city', 'country']].copy()
    
    location_df.drop_duplicates()

    save_file('dim_location.csv', location_df, save_directory)

# creates csv file for time dimensions table
def create_time_dim(data, save_directory):
    # slice copy of original pandas data frame for time dimensions dataframe
    time_df = data[['day','time']].copy()

    time_df = time_df.drop_duplicates()

    # create id column 
    time_df.insert(0, "id", np.arange(len(time_df['day'])), True)
    
    save_file('dim_time.csv', time_df, save_directory)

# creates csv file for values dimensions table
def create_values_dim(data, save_directory):
    # slice copy of original pandas data frame for values dimensions dataframe
    values_df = data[['pm25', 'pm10', 'so2', 'no', 'o3']].copy()

    # create id column 
    values_df.insert(0, "id", np.arange(len(data['pm25'])), True)
    
    save_file('dim_values.csv', values_df, save_directory)

def create_relations(data, save_directory):
    value_data = read_file('relational_data\dim_values.csv')
    time_data = read_file('relational_data\dim_time.csv')
    location_data = read_file('relational_data\dim_location.csv')

    # create relations table using valus DataFrames id's 
    relations_df = value_data[['id']].copy()
    relations_df = relations_df.rename(columns={"id": "value id"})

    # add time DataFrames id column to relations table
    column = []
    for i in range(len(data['day'])):
        val = time_data[(time_data['day'] == data['day'][i]) & (time_data['time'] == data['time'][i])]
        column.append(int(val['id']))
    relations_df.insert(1, "time id", column, True)

    # add location DataFrames id column to relations table
    column = []
    for i in range(len(data['id'])):
        val = location_data[(location_data['id'] == data['id'][i])]
        column.append(int(val['id']))
    relations_df.insert(2, "location id", column, True)
    
    save_file('relations.csv', relations_df, save_directory)

_start_create_relational_database()
