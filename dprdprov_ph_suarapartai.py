### SUARA PARTAI ###
import urllib.parse
import requests
import pandas as pd
import json
import csv
import datetime
import warnings
import numpy as np

warnings.filterwarnings("ignore")
durl_dapil = "https://pemilu2019.kpu.go.id/static/json/dapil/dprdprov/"#+nmr_wilayah
durl_ph = "https://pemilu2019.kpu.go.id/static/json/ph/"#+nmr_wilayah

##buka file partai
json_partai = json.load(open("hasil/partai.json"))

##buka file wilayah
df_wilayah = pd.read_csv("hasil/wilayah_unik.csv", index_col=0)
print(df_wilayah)

##ambil data penetapan hasil
for index, row in df_wilayah.iterrows():
    nmr_wilayah = str(row["nmr_wilayah"])
    nama_wilayah = row["nama_wilayah"]
    print(nmr_wilayah+" "+nama_wilayah+" START:"+str(datetime.datetime.now().strftime("%H:%M")))

    ##ambil data dapil tiap prov
    try:
        url_dapil = durl_dapil+nmr_wilayah+".json"
        json_dapil = requests.get(url_dapil, verify=False).json()
    except Exception as edapil:
        print("error req dapil ("+nmr_wilayah+nama_wilayah+") "+str(edapil))
        row_edapil = nmr_wilayah+","+nama_wilayah+",requestdapil,"+str(edapil)+'\n'
        with open("hasil/dprd_prov_ph_suarapartai/error_ph.csv","a",newline="\n") as fd:
            fd.write(row_edapil)
        continue

    ##ambil data ph
    try:
        url_ph = durl_ph+nmr_wilayah+".json"
        json_ph = requests.get(url_ph, verify=False).json()
    except Exception as eph:
        print("error req ph ("+nmr_wilayah+nama_wilayah+") "+str(eph))
        row_eph = nmr_wilayah+","+nama_wilayah+",requestph,"+str(eph)+'\n'
        with open("hasil/dprd_prov_ph_suarapartai/error_ph.csv","a",newline="\n") as fd:
            fd.write(row_eph)
        continue
    
    ##ambil suara partai
    column = [
        'nmr_dapil','nama_dapil','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20'
        ]
    df_suarapartai = pd.DataFrame(columns=column)
    for a, b in json_ph["table"].items():
        nmr_dapil =  str(a)
        nama_dapil = json_dapil[a]['nama']
        new_row = {
                'nmr_dapil':nmr_dapil,
                'nama_dapil':nama_dapil,
                '1': 0,
                '2': 0,
                '3': 0,
                '4': 0,
                '5': 0,
                '6': 0,
                '7': 0,
                '8': 0,
                '9': 0,
                '10': 0,
                '11': 0,
                '12': 0,
                '13': 0,
                '14': 0,
                '15': 0,
                '16': 0,
                '17': 0,
                '18': 0,
                '19': 0,
                '20': 0
                }
        for c,d in json_ph["table"][nmr_dapil]["partai"].items():
            new_row[c] = d
        df_suarapartai = df_suarapartai.append(new_row, ignore_index=True)

    ##finish
    df_suarapartai.to_csv("hasil/dprd_prov_ph_suarapartai/"+nmr_wilayah+" "+nama_wilayah+".csv")
    print("FINISH:"+str(datetime.datetime.now().strftime("%H:%M")))

print("DONE kak")
