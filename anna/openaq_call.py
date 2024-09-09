import requests
import os
import pandas as pd

API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'

headers = {"Accept": "application/json",
           "X-API-Key": API_KEY}

# file path for saving
cwd = os.path.dirname(os.path.realpath(__file__))
save_directory = os.path.join(cwd, 'data')
os.makedirs(save_directory, exist_ok=True)

wanted_params = {1: 'pm10',
           2: 'pm25',
           3: 'o3',
           6: 'so2',
           19843: 'no'}

# call api with provided location, create csv file with
# meter ID, location name, lat, lon, day, time, pm2.5, pm10, so2, no, o3, save to data
def get_location(location_id):
    url = f'https://api.openaq.org/v2/locations/{location_id}'

    sensor_response = requests.get(url, headers=headers)

    sensor_data = sensor_response.json()
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
        's02': '',
        'no': '',
        'o3': ''
        }

    # update row dict to include available sensors from the ones we're intereted in
    for param in sensor_dict.get('parameters', []):
        if param.get("parameterId") in wanted_params:
                row.update({param['parameter']: param.get('lastValue')})

    # add row to csv file
    df = pd.DataFrame([row])
    file_name = os.path.join(save_directory, 'sensor_data.csv')
    df.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)

if __name__ == '__main__':
    # Madrid, London, Berlin, Rome, Helsinki
    sensors = [4318, 154, 4767, 7529, 4593]
    for sensor in sensors:
        get_location(sensor)