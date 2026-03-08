import requests
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

today = datetime.now()
three_months_ago = today - relativedelta(months=2) # as almost 3 month lag

year = three_months_ago.strftime("%Y")
month = three_months_ago.strftime("%m")

filename = f"yellow_tripdata_{year}-{month}.parquet"
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

response = requests.get(url)

if response.status_code == 200:
    if filename not in os.listdir("data"):
        with open(f"data/{filename}", 'wb') as f:
            f.write(response.content)
    else:
        print(f"{filename} already exists in the data directory.")
else:
    print(f"No data available for the specified {year}_{month}.")
