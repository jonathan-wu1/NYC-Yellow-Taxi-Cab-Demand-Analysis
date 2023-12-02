from urllib.request import urlretrieve
import os
import pandas as pd
import zipfile


output_relative_dir = 'data/landing/'



# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_relative_dir):
    os.makedirs(output_relative_dir)


target_dir = 'yellow_data'
if not os.path.exists(output_relative_dir + target_dir):
    os.makedirs(output_relative_dir + target_dir)

YEAR = '2022'
MONTHS = range(1,13)
URL_TEMPLATE_YELLOW = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"#year-month.parquet


fhv_output_dir = output_relative_dir + 'yellow_data'

for month in MONTHS:
    # 0-fill i.e 1 -> 01, 2 -> 02, etc
    month = str(month).zfill(2) 
    print(f"Begin month {month}")
    
    # generate url
    url = f'{URL_TEMPLATE_YELLOW}{YEAR}-{month}.parquet'
    # generate output location and filename
    output_dir = f"{fhv_output_dir}/{YEAR}-{month}.parquet"
    # download
    urlretrieve(url, output_dir)
    
    print(f"Completed month {month}")



# Downloading the taxi zone data.
output_dir = 'data/taxi_zones'

# check if it exists as it makedir will raise an error if it does exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# generate url
url_1 = f'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
url_2 = f'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip'

# generate output location and filename
file_name_1 = "taxi+_zone_lookup.csv"
file_name_2 = "taxi_zones.zip"

# download
urlretrieve(url_1, output_dir + '/' + file_name_1) 
urlretrieve(url_2, output_dir + '/' + file_name_2) 

zip_file = zipfile.ZipFile(output_dir + '/' + file_name_2)
zip_file.extractall(output_dir)

# Downloading weather data from open-metro API.
WEATHER_URL = "https://archive-api.open-meteo.com/v1/archive?latitude=40.7143&longitude=-74.006&start_date=2022-01-01&end_date=2022-12-31&hourly=temperature_2m,rain,cloudcover,windspeed_10m&timezone=America%2FNew_York&format=csv"

urlretrieve(WEATHER_URL, output_relative_dir + "weather.csv") 


