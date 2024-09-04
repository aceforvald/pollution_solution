import requests
import os
import pandas as pd

def _start_api():
    API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'

    wanted_params = {1: 'pm10',
           2: 'pm25',
           3: 'o3',
           6: 'so2',
           19843: 'no'}

    # Madrid, London, Berlin, Rome, Helsinki
    sensors = [4318, 154, 4767, 7529, 4593]

    # file path for saving
    cwd = os.path.dirname(os.path.realpath(__file__))
    save_directory = os.path.join(cwd, 'data')
    os.makedirs(save_directory, exist_ok=True)

    # save data in same csv file
    for sensor in sensors:
        sensor_data = get_location(sensor, API_KEY, wanted_params)
        save_same_file(sensor_data, wanted_params, save_directory)

        # save data in different csv files grouped by locations
        save_different_files(sensor_data, wanted_params, save_directory)

# call api with provided location id
def get_location(location_id, API_KEY, wanted_params):

    headers = {"Accept": "application/json",
           "X-API-Key": API_KEY}

    url = f'https://api.openaq.org/v2/locations/{location_id}'

    sensor_response = requests.get(url, headers=headers)

    return sensor_response.json()

# saves all the locations in the same csv file
def save_same_file(sensor_data, wanted_params, save_directory):
    sensor_dict = sensor_data.get('results', [])[0]

    # create row dictionary for storing variables to keep
    row = {'id': sensor_dict.get('id', ''),
        'location': sensor_dict.get('name', ''),
        'lat': sensor_dict.get('coordinates', {}).get('latitude', ''),
        'lon': sensor_dict.get('coordinates', {}).get('longitude', ''),
        'day': sensor_dict.get('lastUpdated')[:10],
        'time': sensor_dict.get('lastUpdated')[11:16],
        'pm25': '',
        'pm10': '',
        'so2': '',
        'no': '',
        'o3': ''
        }

    # update row dict to include available sensors from the ones we're intereted in
    for param in sensor_dict.get('parameters', []):
        if param.get("parameterId") in wanted_params:
                row.update({param['parameter']: param.get('lastValue')})

    # add row to csv file
    df = pd.DataFrame([row])
    file_name = os.path.join(save_directory, 'sensor_data_all.csv')
    df.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)

def save_different_files(sensor_data, wanted_params, save_directory):
    sensor_dict = sensor_data.get('results', [])[0]

    # create row dictionary for storing variables to keep
    row = {'id': sensor_dict.get('id', ''),
        'location': sensor_dict.get('name', ''),
        'lat': sensor_dict.get('coordinates', {}).get('latitude', ''),
        'lon': sensor_dict.get('coordinates', {}).get('longitude', ''),
        'day': sensor_dict.get('lastUpdated')[:10],
        'time': sensor_dict.get('lastUpdated')[11:16],
        'pm25': '',
        'pm10': '',
        'so2': '',
        'no': '',
        'o3': ''
        }

    # update row dict to include available sensors from the ones we're intereted in
    for param in sensor_dict.get('parameters', []):
        if param.get("parameterId") in wanted_params:
                row.update({param['parameter']: param.get('lastValue')})

    # add row to csv file
    df = pd.DataFrame([row])
    file_name = os.path.join(save_directory, row['location'] + '_sensor_data.csv')
    df.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)

#comment this when using airflow
_start_api()
