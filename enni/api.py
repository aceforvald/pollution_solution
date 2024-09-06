import requests
import os
import pandas as pd

# TODO add country code

def _start_api():
    API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'

    wanted_params = {1: 'pm10',
           2: 'pm25',
           3: 'o3',
           6: 'so2',
           19843: 'no'}

    sensors_file = pd.read_csv("C:\grad_project\pollution_solution\enni\sensors.csv")
    sensors = sensors_file[sensors_file.columns[0]]
    city = sensors_file[sensors_file.columns[-1]]
    print(sensors)

    # file path for saving
    cwd = os.path.dirname(os.path.realpath(__file__))
    save_directory = os.path.join(cwd, 'data')
    os.makedirs(save_directory, exist_ok=True)

    # save data in same csv file
    i = 0
    for sensor in sensors:
        sensor_data = get_location(sensor, API_KEY, wanted_params)
        save_same_file(sensor_data, wanted_params, save_directory, city[i])
        i = i +1

"""
        # save data in different csv files grouped by locations
        save_different_files(sensor_data, wanted_params, save_directory)
"""

# call api with provided location id
def get_location(location_id, API_KEY, wanted_params):

    headers = {"Accept": "application/json",
           "X-API-Key": API_KEY}

    url = f'https://api.openaq.org/v2/locations/{location_id}'

    sensor_response = requests.get(url, headers=headers)

    return sensor_response.json()

# saves all the locations in the same csv file
def save_same_file(sensor_data, wanted_params, save_directory, city):
    try:
        sensor_dict = sensor_data.get('results', [])[0]
    except:
        print("row skipped, check sensors_all.csv")
        return

    # create row dictionary for storing variables to keep
    row = {'id': sensor_dict.get('id', ''),
        'location': sensor_dict.get('name', ''),
        'city': city,
        'country': sensor_dict.get('country', ''),
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

    print(row['id'])

    file_name = os.path.join(save_directory, 'sensor_data_all.csv')
    df.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)

    # remove duplicates
    df = pd.read_csv(file_name)
    df = df.drop_duplicates()
    df.to_csv(file_name, mode='w', index=False)
"""
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

    # remove duplicates
    df = pd.read_csv(file_name)
    df = df.drop_duplicates()
    df.to_csv(file_name, mode='w', index=False)
"""

#comment this when using airflow
_start_api()
