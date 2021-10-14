import requests
import json
import pandas as pd
from pandas import json_normalize

payload = {'Key': '1dd466067f114baba4a81103211309', 'q': 'Berlin', 'aqi': 'yes'}
r = requests.get("http://api.weatherapi.com/v1/current.json", params=payload)

# Get the json from the request's result
r_string = r.json()

# print the original json 
print (r_string)

# Show the unnormalized dataframe problem
#df = pd.DataFrame.from_dict(r_string, orient="index")
#print(df)

# flatten with normalize function
normalized = json_normalize(r_string)

#print normalized version + datatypes
print(normalized)
print(normalized.dtypes)

