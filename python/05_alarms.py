import pandas as pd
from numpy import float64
import datetime as dt

# Import the new influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

df = pd.read_csv("data/threshold.csv", )

# create a timestamp out of the four columns
# needed for influx 2020-01-01T00:00:00.00Z
# lambda s : dt.datetime(*s) takes every row and parses it -> *s
# strftime to reformat the string into influxdb format
df['TimeStamp'] = df[['year', 'month', 'day', 'hour']].apply(lambda s : dt.datetime(*s).strftime('%Y-%m-%dT%H:%M:%SZ'),axis = 1)

# set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace = True)
# drop the year, month, day, hour, No from the dataframe
converted_ts = df.drop(['year', 'month', 'day', 'hour','No'], axis=1)
print(converted_ts)

# Change the column types to float
ex_df = converted_ts.astype({"PM2.5": float64,
               "PM10": float64,
               "SO2": float64,
               "NO2": float64,
               "CO": float64,
               "O3": float64,
               "TEMP": float64,
               "PRES": float64,
               "DEWP": float64,
               "RAIN": float64,
               "WSPM": float64 })


Fields = ['PM2.5','PM10','SO2','NO2','CO','O3','TEMP','PRES','DEWP','RAIN','wd','WSPM']
# Define tag fields
datatags = ['station','wd']


client = influxdb_client.InfluxDBClient(
   url='http://localhost:8086',
   token='lTUKuRE46dJw8Yj_AmYtQHELsnfNM1eGVdJkYUj_Q_Ddq7yqCScDlbt9PYdu-RR_OW-NX9S_GaxNqXz7iAECCw==',
   org='my-org'
)

#Write the data with two tags
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='air-quality',org='my-org',record = ex_df, data_frame_measurement_name = 'location-tag-only', data_frame_tag_columns=['station','wd'])
print(message)

write_api.flush()







# old API
#client = DataFrameClient('localhost', 8086, 'my-admin', 'my-password', 'my-bucket')
# Write data to "SchoolData" measurement of "schooldb" database.
#client.write_points(ex_df,"db0",tag_columns=datatags,protocol='line')

