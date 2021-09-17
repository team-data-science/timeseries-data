
from numpy import float64
import pandas as pd
import datetime as dt

df = pd.read_csv("data/test.csv", )
df_big = pd.read_csv("data/PRSA_Data_Aotizhongxin_20130301-20170228.csv", )

# create a timestamp out of the four columns
# needed for influx 2020-01-01T00:00:00.00Z
# lambda s : dt.datetime(*s) takes every row and parses it -> *s
# strftime to reformat the string into influxdb format
df['TimeStamp'] = df[['year', 'month', 'day', 'hour']].apply(lambda s : dt.datetime(*s).strftime('%Y-%m-%dT%H:%M:%SZ'),axis = 1)

# set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace = True)
# drop the year, month, day, hour, No from the dataframe
wrong_types_df = df.drop(['year', 'month', 'day', 'hour','No'], axis=1)

print("test.csv")
print(wrong_types_df.dtypes)

print()
print("PRSA_Data_Aotizhongxin_20130301-20170228.csv")
print(df_big.dtypes)

right_types = wrong_types_df.astype({"PM2.5": float64,
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

print()
print("test.csv - Fixed data types - ")
print(right_types.dtypes)