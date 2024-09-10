import requests
import os
import pandas as pd
from pandas import json_normalize

# file path for saving
cwd = os.path.dirname(os.path.realpath(__file__))
save_directory = os.path.join(cwd, 'data')
os.makedirs(save_directory, exist_ok=True)

def add_sensors(lat, lon):
    '''
    Get data about existing sensors within a radius of specified lat/lon.

    Returns:
    DataFrame containing sensor information, including location, available parameters, and sensor ID.
    If there's an error, returns empty DataFrame.
    '''
            
    API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'
    base_url = 'https://api.openaq.org/v2/locations'

    headers = {
        "Accept": "application/json",
        "X-API-Key": API_KEY}

    # parameters for API request (lat and lon provided in function call)
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
    
    try:
        # Get all sensor data for lat lon pair
        add_response = requests.get(base_url, params=params, headers=headers)
        if add_response.status_code == 200:
            add_data = add_response.json()

            print(f'Number of sensors: {len(add_data['results'])}')

            # Ensure location has any sensors
            if 'results' not in add_data or len(add_data['results']) == 0:
                print(f'Uh oh, no results key in API response')
                return pd.DataFrame()            
            
            add_df = json_normalize(add_data['results'])

            # drop columns not needed
            add_df = add_df.drop(columns=['entity',
                                                'sources',
                                                'isMobile',
                                                'isAnalysis',
                                                'sensorType',
                                                'measurements',
                                                'bounds',
                                                'manufacturers'])
            
            # drop rows of sensors that have no parameters, if no sensors with parameters
            # remain, return empty dataframe
            add_df = add_df.dropna(subset=['parameters'])
            if add_df.empty:
                return pd.DataFrame()
            
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
            
            # TODO: why on earth are these suddenly not ints?!
            add_df = add_df.infer_objects()

            return add_df
        
    except requests.exceptions.RequestException as e:
        print(f'API call failure: {e} CHECK {add_df.columns}')
        return pd.DataFrame()

    except ValueError as e:
        print(f'JSON failure: {e} CHECK {add_df.columns}')
        return pd.DataFrame()  

def get_sensors(lat, lon, metro, country, sensors_df):
    '''
    Pass new lat and lon with build df, append new df to existing. Save
    resulting dataframe to sensors.csv in data folder.

    Parameters:
    lat
    lon
    metro: metro area that coordinates center
    country: country of metro area
    sensors_df: dataframe that contains previously collected sensors

    Returns:
    DataFrame with all locations called thus far.
    '''

    # append result of add_sensors to previous results of the call
    add_sensors_df = add_sensors(lat, lon)

    if add_sensors_df is None:
        # or add_sensors_df.empty():
        print(f'No data for {metro}')
        return sensors_df

    # append columns with metro area and country from calling function
    add_sensors_df['metroArea'] = metro
    add_sensors_df['country'] = country
    
    sensors_df = pd.concat([sensors_df, add_sensors_df], ignore_index=True)
    
    # save as csv (w/o original parameters list), return dataframe (w original parameters list)
    sensors_to_csv = sensors_df.drop(columns=['parameters'])
    file_name = os.path.join(save_directory, 'sensors.csv')
    sensors_to_csv.to_csv(file_name, mode='w', index=False)
    return sensors_df

if __name__ == '__main__':
    test_locations = {'Madrid': [40.42188, -3.70079, 'ES'],
                 'Berlin': [52.51998, 13.38695, 'GE'],
                 'London': [51.51081, -0.14263, 'GB'],
                 'Rome': [41.89964, 12.47248, 'IT'],
                 'Helsinki': [60.19637, 24.93295, 'FI']}

    eu_capitals = {
                'Tiranë': [41.3305141, 19.825562857582966, 'AL'],
                'Andorra la Vella': [42.5069391, 1.5212467, 'AD'],
                'Yerevan': [40.1776245, 44.5126174, 'AM'],
                'Vienna': [48.2083537, 16.3725042, 'AT'],
                'Baku City': [40.3755885, 49.8328009, 'AZ'],
                'Minsk': [53.9024716, 27.5618225, 'BY'],
                'Brussels': [50.8465573, 4.351697, 'BE'],
                'City of Sarajevo': [43.8519774, 18.3866868, 'BA'],
                'Sofia': [42.6977028, 23.3217359, 'BG'],
                'Zagreb': [45.84264135, 15.962231476593626, 'HR'],
                'Nicosia': [35.1748976, 33.3638568, 'CY'],
                'Prague': [50.0874654, 14.4212535, 'CZ'],
                'Copenhagen': [55.6867243, 12.5700724, 'DK'],
                'Tallinn': [59.4372155, 24.7453688, 'EE'],
                'Helsinki': [60.1674881, 24.9427473, 'FI'],
                'Paris': [48.8588897, 2.3200410217200766, 'FR'],
                'Tbilisi': [41.6934591, 44.8014495, 'GE'],
                'Berlin': [52.5170365, 13.3888599, 'DE'],
                'Athens': [37.9839412, 23.7283052, 'GR'],
                'Budapest': [47.4979937, 19.0403594, 'HU'],
                'Reykjavik': [64.145981, -21.9422367, 'IS'],
                'Dublin': [53.3498006, -6.2602964, 'IE'],
                'Rome': [41.8933203, 12.4829321, 'IT'],
                'Astana': [51.1282205, 71.4306682, 'KZ'],
                'Riga': [56.9493977, 24.1051846, 'LV'],
                'Vaduz': [47.1392862, 9.5227962, 'LI'],
                'Vilnius': [54.6870458, 25.2829111, 'LT'],
                'Luxembourg': [49.6112768, 6.129799, 'LU'],
                'Valletta': [35.8989818, 14.5136759, 'MT'],
                'Chișinau': [47.0245117, 28.8322923, 'MD'],
                'Monaco': [43.7311424, 7.4197576, 'MC'],
                'Podgorica': [42.4415238, 19.2621081, 'ME'],
                'Amsterdam': [52.3727598, 4.8936041, 'NL'],
                'Skopje': [41.9960924, 21.4316495, 'MK'],
                'Oslo': [59.9133301, 10.7389701, 'NO'],
                'Warsaw': [52.2319581, 21.0067249, 'PL'],
                'Lisbon': [38.7077507, -9.1365919, 'PT'],
                'Bucharest': [44.4361414, 26.1027202, 'RO'],
                'Moscow': [55.7504461, 37.6174943, 'RU'],
                'City of San Marino': [43.9363996, 12.4466991, 'SM'],
                'Belgrade': [44.8178131, 20.4568974, 'RS'],
                'Bratislava': [48.1435149, 17.108279, 'SK'],
                'Ljubljana': [46.0500268, 14.5069289, 'SI'],
                'Madrid': [40.4167047, -3.7035825, 'ES'],
                'Stockholm': [59.3251172, 18.0710935, 'SE'],
                'Bern': [46.9482713, 7.4514512, 'CH'],
                'Ankara': [39.9207886, 32.8540482, 'TR'],
                'Kyiv': [50.4500336, 30.5241361, 'UA'],
                'London': [51.5073219, -0.1276474, 'GB'],
                'Vatican City': [41.9034912, 12.4528349, 'VA']}

    eu_1_million_plus = {'Moscow': [55.7504461, 37.6174943, 'RU'],
                'Istanbul': [41.0091982, 28.9662187, 'TR'],
                'London': [51.5073219, -0.1276474, 'GB'],
                'Saint Petersburg': [59.938732, 30.316229, 'RU'],
                'Berlin': [52.5170365, 13.3888599, 'DE'],
                'Madrid': [40.4167047, -3.7035825, 'ES'],
                'Kyiv': [50.4500336, 30.5241361, 'UA'],
                'Rome': [41.8933203, 12.4829321, 'IT'],
                'Baku City': [40.3755885, 49.8328009, 'AZ'],
                'Paris': [48.8588897, 2.3200410217200766, 'FR'],
                'Vienna': [48.2083537, 16.3725042, 'AT'],
                'Minsk': [53.9024716, 27.5618225, 'BY'],
                'Hamburg': [53.550341, 10.000654, 'DE'],
                'Warsaw': [52.2319581, 21.0067249, 'PL'],
                'Bucharest': [44.4361414, 26.1027202, 'RO'],
                'Belgrade': [44.8178131, 20.4568974, 'RS'],
                'Budapest': [47.4979937, 19.0403594, 'HU'],
                'Barcelona': [41.3828939, 2.1774322, 'ES'],
                'Munich': [48.1371079, 11.5753822, 'DE'],
                'Kharkiv': [49.9923181, 36.2310146, 'UA'],
                'Prague': [50.0874654, 14.4212535, 'CZ'],
                'Milan': [45.4641943, 9.1896346, 'IT'],
                'Kazan': [55.7823547, 49.1242266, 'RU'],
                'Sofia': [42.6977028, 23.3217359, 'BG'],
                'Tbilisi': [41.6934591, 44.8014495, 'GE'],
                'Nizhny Novgorod': [56.3264816, 44.0051395, 'RU'],
                'Ufa': [54.7261409, 55.947499, 'RU'],
                'Samara': [53.198627, 50.113987, 'RU'],
                'Birmingham': [52.4796992, -1.9026911, 'GB'],
                'Rostov-on-Don': [47.2213858, 39.7114196, 'RU'],
                'Krasnodar': [45.0352718, 38.9764814, 'RU'],
                'Yerevan': [40.1776245, 44.5126174, 'AM'],
                'Cologne': [50.938361, 6.959974, 'DE'],
                'Voronezh': [51.6605982, 39.2005858, 'RU'],
                'Perm District': [58.014965, 56.246723, 'RU'],
                'Volgograd': [48.7081906, 44.5153353, 'RU'],
                'Odesa': [46.4843023, 30.7322878, 'UA']}

    def get_set(cities):
        all_sensors = pd.DataFrame()
        for city in cities:
            print(f'LOOK AT: {city}')
            all_sensors = get_sensors(cities[city][0], cities[city][1], city, cities[city][2], all_sensors)
        return all_sensors
    
    sensors = get_set(eu_capitals)

    print(sensors)

    # get a list of ids to iterate over
    list_of_ids = sensors['id'].tolist()
    print(list_of_ids)