import pandas as pd
import influxdb
from influxdb import DataFrameClient
import datetime as dt

# Read the csv into a dataframe
df = pd.read_csv("data/test.csv", )
print(df)

# create a timestamp out of the four columns
df['TimeStamp'] = df[['year', 'month', 'day', 'hour']].apply(lambda s : dt.datetime(*s),axis = 1)

# set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace = True)
# drop the year, month, day, hour, No from the dataframe
ex_df = df.drop(['year', 'month', 'day', 'hour','No'], axis=1)
print(ex_df)


Fields = ['PM2.5','PM10','SO2','NO2','CO','O3','TEMP','PRES','DEWP','RAIN','wd','WSPM']
# Define tag fields
datatags = ['station','wd']



client = DataFrameClient('localhost', 8086, 'admin', 'admin', 'db0')
# Write data to "SchoolData" measurement of "schooldb" database.
client.write_points(ex_df,"db0",tag_columns=datatags,protocol='line')

