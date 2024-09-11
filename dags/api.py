import requests
import os
import pandas as pd

# counters
checked = 0
skipped = 0
entered = 0

run_again = []

def _start_api():
    API_KEY = '7b2fbb569507ae14141d3de0ff76044999ae3859816024ab9d2978c83bc3d804'

    wanted_params = {1: 'pm10',
           2: 'pm25',
           3: 'o3',
           6: 'so2',
           19843: 'no'}

    cwd = os.path.dirname(os.path.realpath(__file__))
    data_directory = os.path.join(cwd, 'data')
    try:
        os.makedirs(data_directory, exist_ok=True)
    except Exception as e:
        print(e)

    try:
        file_name = os.path.join(data_directory, 'sensors.csv')
        sensors_file = pd.read_csv(file_name)
    except Exception as e:
        print(e)

    sensors = sensors_file[sensors_file.columns[0]]
    city = sensors_file[sensors_file.columns[-1]]
    print(f'Checking {sensors.count()} sensors:')

    # save data in same csv file
    i = 0
    for sensor in sensors:
        sensor_data = get_location(sensor, API_KEY, wanted_params)
        save_same_file(sensor_data, wanted_params, data_directory, city[i])
        i = i +1

# call api with provided location id
def get_location(location_id, API_KEY, wanted_params):

    headers = {"Accept": "application/json",
           "X-API-Key": API_KEY}

    url = f'https://api.openaq.org/v2/locations/{location_id}'

    try:
        sensor_response = requests.get(url, headers=headers)
    except Exception as e:
        print(e)

    return sensor_response.json()


# saves all the locations in the same csv file
def save_same_file(sensor_data, wanted_params, save_directory, city):
    
    global checked
    global skipped
    global entered

    global run_again

    try:
        sensor_dict = sensor_data.get('results', [])[0]
        checked += 1
    except:
        print("row skipped, check sensors_all.csv")
        skipped += 1
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
                entered += 1

    # add row to csv file
    df = pd.DataFrame([row])

    print(row['id'])

    try:
        file_name = os.path.join(save_directory, 'sensor_data_all.csv')
        df.to_csv(file_name, mode='a', header=not pd.io.common.file_exists(file_name), index=False)
    except Exception as e:
        print(e)

    # remove duplicates
    try:
        df = pd.read_csv(file_name)
        run_again.append(row['id'])
        df = df.drop_duplicates()
        df.to_csv(file_name, mode='w', index=False)
    except Exception as e:
        print(e)
    


#comment this when using airflow

_start_api()

print(f'checked: {checked}, skipped: {skipped}, entered: {entered}')