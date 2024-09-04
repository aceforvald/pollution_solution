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
    test_locations = {'Madrid': [40.42188, -3.70079],
                 'Berlin': [52.51998, 13.38695],
                 'London': [51.51081, -0.14263],
                 'Rome': [41.89964, 12.47248],
                 'Helsinki': [60.19637, 24.93295]}

    eu_capitals = {'Tiranë': [41.3305141, 19.825562857582966],
                'Andorra la Vella': [42.5069391, 1.5212467],
                'Yerevan': [40.1776245, 44.5126174],
                'Vienna': [48.2083537, 16.3725042],
                'Baku City': [40.3755885, 49.8328009],
                'Minsk': [53.9024716, 27.5618225],
                'Brussels': [50.8465573, 4.351697],
                'City of Sarajevo': [43.8519774, 18.3866868],
                'Sofia': [42.6977028, 23.3217359],
                'Zagreb': [45.84264135, 15.962231476593626],
                'Nicosia': [35.1748976, 33.3638568],
                'Prague': [50.0874654, 14.4212535],
                'Copenhagen': [55.6867243, 12.5700724],
                'Tallinn': [59.4372155, 24.7453688],
                'Helsinki': [60.1674881, 24.9427473],
                'Paris': [48.8588897, 2.3200410217200766],
                'Tbilisi': [41.6934591, 44.8014495],
                'Berlin': [52.5170365, 13.3888599],
                'Athens': [37.9839412, 23.7283052],
                'Budapest': [47.4979937, 19.0403594],
                'Reykjavik': [64.145981, -21.9422367],
                'Dublin': [53.3498006, -6.2602964],
                'Rome': [41.8933203, 12.4829321],
                'Astana': [51.1282205, 71.4306682],
                'Riga': [56.9493977, 24.1051846],
                'Vaduz': [47.1392862, 9.5227962],
                'Vilnius': [54.6870458, 25.2829111],
                'Luxembourg': [49.6112768, 6.129799],
                'Valletta': [35.8989818, 14.5136759],
                'Chișinau': [47.0245117, 28.8322923],
                'Monaco': [43.7311424, 7.4197576],
                'Podgorica': [42.4415238, 19.2621081],
                'Amsterdam': [52.3727598, 4.8936041],
                'Skopje': [41.9960924, 21.4316495],
                'Oslo': [59.9133301, 10.7389701],
                'Warsaw': [52.2319581, 21.0067249],
                'Lisbon': [38.7077507, -9.1365919],
                'Bucharest': [44.4361414, 26.1027202],
                'Moscow': [55.7504461, 37.6174943],
                'City of San Marino': [43.9363996, 12.4466991],
                'Belgrade': [44.8178131, 20.4568974],
                'Bratislava': [48.1435149, 17.108279],
                'Ljubljana': [46.0500268, 14.5069289],
                'Madrid': [40.4167047, -3.7035825],
                'Stockholm': [59.3251172, 18.0710935],
                'Bern': [46.9482713, 7.4514512],
                'Ankara': [39.9207886, 32.8540482],
                'Kyiv': [50.4500336, 30.5241361],
                'London': [51.5073219, -0.1276474],
                'Vatican City': [41.9034912, 12.4528349]}

    eu_1_million_plus = {'Moscow': [55.7504461, 37.6174943],
                'Istanbul': [41.0091982, 28.9662187],
                'Saint Petersburg': [59.938732, 30.316229],
                'Berlin': [52.5170365, 13.3888599],
                'Kyiv': [50.4500336, 30.5241361],
                'Rome': [41.8933203, 12.4829321],
                'Baku City': [40.3755885, 49.8328009],
                'Paris': [48.8588897, 2.3200410217200766],
                'Vienna': [48.2083537, 16.3725042],
                'New York County': [40.7127281, -74.0060152],
                'London': [51.5073219, -0.1276474],
                'Madrid': [40.4167047, -3.7035825],
                'Minsk': [53.9024716, 27.5618225],
                'Hamburg': [53.550341, 10.000654],
                'Warsaw': [52.2319581, 21.0067249],
                'Bucharest': [44.4361414, 26.1027202],
                'Belgrade': [44.8178131, 20.4568974],
                'Budapest': [47.4979937, 19.0403594],
                'Barcelona': [41.3828939, 2.1774322],
                'Munich': [48.1371079, 11.5753822],
                'Kharkiv': [49.9923181, 36.2310146],
                'Prague': [50.0874654, 14.4212535],
                'Milan': [45.4641943, 9.1896346],
                'Kazan': [55.7823547, 49.1242266],
                'Sofia': [42.6977028, 23.3217359],
                'Tbilisi': [41.6934591, 44.8014495],
                'Nizhny Novgorod': [56.3264816, 44.0051395],
                'Ufa': [54.7261409, 55.947499],
                'Samara': [53.198627, 50.113987],
                'Birmingham': [52.4796992, -1.9026911],
                'Rostov-on-Don': [47.2213858, 39.7114196],
                'Krasnodar': [45.0352718, 38.9764814],
                'Yerevan': [40.1776245, 44.5126174],
                'Cologne': [50.938361, 6.959974],
                'Voronezh': [51.6605982, 39.2005858],
                'Perm District': [58.014965, 56.246723],
                'Volgograd': [48.7081906, 44.5153353],
                'Odesa': [46.4843023, 30.7322878]}

    #all_sensors = pd.DataFrame()

    def get_set(cities):
        all_sensors = pd.DataFrame()
        for city in cities:
            all_sensors = get_sensors(cities[city][0], cities[city][1], city, all_sensors)
        return all_sensors
    
    sensors = get_set(test_locations)

    print(sensors)

    # get a list of ids to iterate over
    list_of_ids = sensors['id'].tolist()
    print(list_of_ids)