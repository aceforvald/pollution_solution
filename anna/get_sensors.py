import requests
import pandas as pd
from pandas import json_normalize

API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'
base_url = 'https://api.openaq.org/v2/locations'

headers = {
    "Accept": "application/json",
    "X-API-Key": API_KEY}

def add_sensors(lat, lon):
    params = {
        'limit': 100,
        'page': 1,
        'offset': 0,
        'sort': 'desc',
        'coordinates': f'{lat},{lon}',
        'radius': 10000,
        'order_by': 'lastUpdated',
        'dump_raw': 'false'
    }

    add_response = requests.get(base_url, params=params, headers=headers)
    if add_response.status_code == 200:
        add_data = add_response.json()

        add_df = json_normalize(add_data['results'])
        add_df = add_df.drop(columns=['entity',
                                              'sources',
                                              'isMobile',
                                              'isAnalysis',
                                              'sensorType',
                                              'measurements',
                                              'bounds',
                                              'manufacturers'])
        
        # add column with all parameters of sensor
        add_df['parameterIds'] = add_df['parameters'].apply(lambda x: [param_dict['id'] for param_dict in x])

        # add columns for parameters we care about
        add_df['has_pm25'] = add_df['parameterIds'].apply(lambda x: 1 if 2 in x else 0)
        add_df['has_pm10'] = add_df['parameterIds'].apply(lambda x: 1 if 1 in x else 0)
        add_df['has_so2'] = add_df['parameterIds'].apply(lambda x: 1 if 6 in x else 0)
        add_df['has_no'] = add_df['parameterIds'].apply(lambda x: 1 if 19843 in x else 0)
        add_df['has_o3'] = add_df['parameterIds'].apply(lambda x: 1 if 3 in x else 0)

        # how many parameters that we care about does this sensor have?
        add_df['sensorCount'] = add_df['has_pm25'] + add_df['has_pm10'] + add_df['has_so2'] + add_df['has_no'] + add_df['has_o3']
        
        return(add_df)
    
    else:
        print('Error {response.status_code}: {response.text}')
        return pd.DataFrame()

# pass new lat and lon with built df, append new df to existing
def get_sensors(lat, lon, metro, sensors_df):
    # append result of add_sensors to previous results of the call - in a dataframe?
    add_sensors_df = add_sensors(lat, lon)

    add_sensors_df['metroArea'] = metro
    
    sensors_df = pd.concat([sensors_df, add_sensors_df], ignore_index=True)
    
    return sensors_df
    
if __name__ == '__main__':
    locations = {'Madrid': [40.42188, -3.70079],
                 'Berlin': [52.51998, 13.38695],
                 'London': [51.51081, -0.14263],
                 'Rome': [41.89964, 12.47248],
                 'Helsinki': [60.19637, 24.93295]}

    all_sensors = pd.DataFrame()

    for city in locations:
        all_sensors = get_sensors(locations[city][0], locations[city][1], city, all_sensors)
    
    print(all_sensors)

    # get a list of ids to iterate over
    list_of_ids = all_sensors['id'].tolist()
    print(list_of_ids)