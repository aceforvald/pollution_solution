import os
import pandas as pd
import numpy as np

def _start_create_relational_database():

    # create relational_data folder for new csv files
    cwd = os.path.dirname(os.path.realpath(__file__))
    save_directory = os.path.join(cwd, 'relational_data')
    os.makedirs(save_directory, exist_ok=True)

    # read data from csv file created in api.py
    sensor_data = read_file('data\sensor_data_all.csv')

    # create new csv files for relational databases tables in right order
    create_country_dim(sensor_data, save_directory)
    create_city_dim(sensor_data, save_directory)
    create_location_dim(sensor_data, save_directory)
    create_time_dim(sensor_data, save_directory)
    create_values_dim(sensor_data, save_directory)
    create_relations(sensor_data, save_directory)

# reads csv file and returns it as a pandas DataFrame
def read_file(path):
    cwd = os.path.dirname(os.path.realpath(__file__))
    directory = os.path.join(cwd, path)
    data = pd.read_csv(directory)
    return data

# saves DataFrame as a csv file
def save_file(file_name, df, save_directory):
    name = os.path.join(save_directory, file_name)
    df.to_csv(name, mode='w', index=False)

# creates csv file for city dimensions table
def create_city_dim(data, save_directory):
    country_data = read_file('relational_data\dim_country.csv')

    # slice copy of original pandas data frame for city dimensions dataframe
    city_df = data[['city', 'country']].copy()
    
    city_df = city_df.drop_duplicates().reset_index(drop=True)

    # create id column 
    city_df.insert(0, "city id", np.arange(len(city_df['city'])), True)

    # add country id column to city dimensions table
    column = []
    for i in range(len(city_df['country'])):
        val = country_data[(country_data['Abbreviation'] == city_df['country'][i]) & (country_data['Abbreviation'] == city_df['country'][i])]
        column.append(val['id'].values[0])
    
    city_df.insert(2, "country id", column, True)
    city_df = city_df.drop('country', axis=1)

    save_file('dim_city.csv', city_df, save_directory)

# creates csv file for country dimensions table
def create_country_dim(data, save_directory):
    # read data for gdp and population
    gdp_data = read_file('world-data-2023.csv')

    # make empty country dataframe with columns
    columns_list = ['id'] + list(gdp_data.columns)
    df = pd.DataFrame(columns=columns_list)

    # slice copy of original pandas data frame for country dimensions dataframe
    country_df = data[['country']].copy()
    country_df = country_df.drop_duplicates().reset_index(drop=True)

    # add gdp column to country dimension table
    for i in range(len(country_df['country'])):
        row = []
        row.append(i)
        val = gdp_data[(gdp_data['Abbreviation'] == country_df['country'][i]) & (gdp_data['Abbreviation'] == country_df['country'][i])]

        for j in range(len(columns_list) -1):
            row.append(val[columns_list[j+1]].values[0])

        df.loc[len(df)] = row

    save_file('dim_country.csv', df, save_directory)

# creates csv file for location dimensions table
def create_location_dim(data, save_directory):
    city_data = read_file('relational_data\dim_city.csv')

    # slice copy of original pandas data frame for location dimensions dataframe
    location_df = data[['id','location', 'lat', 'lon', 'city']].copy()
    
    location_df = location_df.drop_duplicates().reset_index(drop=True)

    # add city id column to location dimensions table
    column = []
    for i in range(len(location_df['city'])):
        val = city_data[(city_data['city'] == location_df['city'][i]) & (city_data['city'] == location_df['city'][i])]
        column.append(val['city id'].values[0])
    
    location_df.insert(1, "city id", column, True)
    location_df = location_df.drop('city', axis=1)

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

    # add time id column to relations table
    column = []
    for i in range(len(data['day'])):
        val = time_data[(time_data['day'] == data['day'][i]) & (time_data['time'] == data['time'][i])]
        column.append(val['id'].values[0])
    relations_df.insert(1, "time id", column, True)

    # add location id column to relations table
    column = []
    for i in range(len(data['id'])):
        val = location_data[(location_data['id'] == data['id'][i])]
        column.append(val['id'].values[0])
    relations_df.insert(2, "location id", column, True)
    
    save_file('relations.csv', relations_df, save_directory)

_start_create_relational_database()
