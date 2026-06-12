from httpx import request
import requests
import openai
import dotenv
import os
import time
import csv

dotenv.load_dotenv()

Polygon_API_KEY = os.getenv("POLYGON_API_KEY")

# print(Polygon_API_KEY)

LIMIT = 1000
url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={Polygon_API_KEY}"   

response=requests.get(url)
step=1
# print(response.json())
data = response.json()
# print(data.keys())
# print(data['next_url'])

tickers = []
for ticker in data["results"]:
    tickers.append(ticker)
# print(len(tickers))

while 'next_url' in data:
    url=data["next_url"]+f'&apikey={Polygon_API_KEY}'
    response=requests.get(url)
    # print(response.json().keys())
    if 'error' in response.json().keys():
        print(response.json()['error'])
        time.sleep(60)
        continue
    data=response.json()
    tickers.extend([ticker for ticker in data["results"]])
    print(step)
    step+=1
print(step)
print(len(tickers))

output_csv="tickers.csv"
headers = tickers[0].keys()

with open(output_csv,"w",newline="",encoding="utf-8") as file:
    writer = csv.DictWriter(file,fieldnames=headers)
    writer.writeheader()
    writer.writerows(tickers)
