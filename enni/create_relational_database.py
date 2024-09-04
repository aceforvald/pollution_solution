import os
import pandas as pd
import numpy as np

def _start_create_relational_database():

    cwd = os.path.dirname(os.path.realpath(__file__))
    save_directory = os.path.join(cwd, 'relational_data')
    os.makedirs(save_directory, exist_ok=True)

    data = read_file()
    create_location_dim(data, save_directory)
    create_time_dim(data, save_directory)
    create_values_dim(data, save_directory)
    create_relations(data, save_directory)

def read_file():
    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, 'data\sensor_data_all.csv')
    data = pd.read_csv(directory)
    return data

def create_location_dim(data, save_directory):
    location_df = data[['id','location', 'lat', 'lon']].copy()
    
    location_df.drop_duplicates()
    file_name = os.path.join(save_directory, 'dim_location.csv')
    location_df.to_csv(file_name, mode='w', index=False)

def create_time_dim(data, save_directory):

    # TODO split day and month as different columns

    time_df = data[['day','time']].copy()

    time_df = time_df.drop_duplicates()
    time_df.insert(0, "id", np.arange(len(time_df['day'])), True)
    
    file_name = os.path.join(save_directory, 'dim_time.csv')
    time_df.to_csv(file_name, mode='w', index=False)

def create_values_dim(data, save_directory):
    values_df = data[['pm25', 'pm10', 'so2', 'no', 'o3']].copy()
    values_df.insert(0, "id", np.arange(len(data['pm25'])), True)
    
    file_name = os.path.join(save_directory, 'dim_values.csv')
    values_df.to_csv(file_name, mode='w', index=False)

def create_relations(data, save_directory):
    # TODO create function for open csvs
    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, 'relational_data\dim_location.csv')
    location_data = pd.read_csv(directory)

    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, 'relational_data\dim_time.csv')
    time_data = pd.read_csv(directory)

    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, 'relational_data\dim_values.csv')
    value_data = pd.read_csv(directory)

    relations_df = value_data[['id']].copy()
    relations_df = relations_df.rename(columns={"id": "value id"})

    column = []
    for i in range(len(data['day'])):
        val = time_data[(time_data['day'] == data['day'][i]) & (time_data['time'] == data['time'][i])]
        column.append(int(val['id']))
    relations_df.insert(1, "time id", column, True)

    column = []
    for i in range(len(data['id'])):
        val = location_data[(location_data['id'] == data['id'][i])]
        print(val)
        column.append(int(val['id']))
    relations_df.insert(2, "location id", column, True)
    
    file_name = os.path.join(save_directory, 'relations.csv')
    relations_df.to_csv(file_name, mode='w', index=False)

_start_create_relational_database()
