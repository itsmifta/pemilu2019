import urllib.parse
import requests
import pandas as pd
import json
import csv

url_dapildprri = 'https://pemilu2019.kpu.go.id/static/json/dapil/dprri.json'

json_dapildprri = requests.get(url_dapildprri, verify=False).json()
with open('hasil/dapildprri.json', 'w') as outfile:
    json.dump(json_dapildprri, outfile)
    
df = pd.read_json('hasil/dapildprri.json')
df.to_csv("hasil/dapildprri.csv")

print("DONE-dapildprri")
