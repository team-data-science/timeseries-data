from numpy import float64, int32, string_
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt

# Import the new influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


# load the configuration from the json file
with open("api_config.json") as json_data_file:
    config = json.load(json_data_file)


payload = {'Key': config['Key'], 'q': 'Berlin', 'aqi': 'no'}
r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

# Get the json
r_string = r.json()
print(r_string)

# normalize the nested json
normalized = json_normalize(r_string)

# you only get the localized time that's why timestamp format with +02.00 is very important (for Berlin) 
# otherwise TS will be in UTC and therefore in the future -> it will not get shown on the board
normalized['TimeStamp'] = normalized['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%S+02:00'))

# rename the columns
normalized.rename(columns={'location.name': 'location', 
      'location.region': 'region',
      'current.temp_c': 'temp_c',
      'current.wind_kph': 'wind_kph'
      }, inplace=True)     
print(normalized)
print(normalized.dtypes)

# set the index to the new timestamp
normalized.set_index('TimeStamp', inplace = True)

# filter out just the temp and wind for export
ex_df = normalized.filter(['temp_c','wind_kph'])      

print(ex_df)
print(ex_df.dtypes)

client = influxdb_client.InfluxDBClient(
   url='http://localhost:8086',
   token='lTUKuRE46dJw8Yj_AmYtQHELsnfNM1eGVdJkYUj_Q_Ddq7yqCScDlbt9PYdu-RR_OW-NX9S_GaxNqXz7iAECCw==',
   org='my-org'
)

#write the test data into measurement
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='live_weather',org='my-org',record = ex_df, data_frame_measurement_name = 'api')
write_api.flush()
print(message)
