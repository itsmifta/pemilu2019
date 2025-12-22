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
durl_hhcw = "https://pemilu2019.kpu.go.id/static/json/hhcw/dprdprov/"#+nmr_wilayah+nmr_kab+nmr_kec

##buka file partai
json_partai = json.load(open("hasil/partai.json"))

##buka file wilayah
df_wilayah = pd.read_csv("hasil/wilayah_unik.csv", index_col=0)
print(df_wilayah)

##ambil data hhcww
for index, row in df_wilayah.iterrows():
    nmr_wilayah = str(row["nmr_wilayah"])
    nama_wilayah = row["nama_wilayah"]
    print(nmr_wilayah+" "+nama_wilayah+" START:"+str(datetime.datetime.now().strftime("%H:%M")))

    ##ambil data daerah tiap wilayah/prov
    df_daerah = pd.read_csv("hasil/daerah_perwilayah/"+nmr_wilayah+" "+nama_wilayah+".csv", index_col=0)
    df_daerah = df_daerah.drop(df_daerah.columns[[0]], axis=1)
    df_daerah_kec = df_daerah.drop(df_daerah.columns[[4,5]], axis=1)
    df_daerah_kec.drop_duplicates(inplace=True)
    df_daerah_kec.reset_index(inplace=True, drop=True)
    column = [
        'nmr_kel','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','19','20'
        ]
    df_suarapartai = pd.DataFrame(columns=column)

    ##ambil data hhcw
    nama_kab=""
    for index2, row2 in df_daerah_kec.iterrows():
        nama_kab_before = nama_kab
        nmr_kab = str(row2['nmr_kab'])
        nama_kab = row2['nama_kab']
        if nama_kab != nama_kab_before:
            print(nama_kab)
        nmr_kec = str(row2['nmr_kec'])
        nama_kec = str(row2['nama_kec'])
        try:
            url_hhcw = durl_hhcw+nmr_wilayah+"/"+nmr_kab+"/"+nmr_kec+".json"
            json_hhcw = requests.get(url_hhcw, verify=False).json()
        except Exception as e_hhcw:
            print("error req hhcw ("+nmr_wilayah+nama_wilayah+") "+str(e_hhcw))
            row_er_hhcw = nmr_wilayah+','+nama_wilayah+','+nmr_kab+','+nmr_kec+','+str(e_hhcw)+'\n'
            with open('hasil/dprd_prov_hhcw_suarapartai/error_hhcw.csv','a',newline='\n') as fd:
                fd.write(row_er_hhcw)
        
        ##ambil suara partai
        for a, b in json_hhcw["table"].items():
            nmr_kel=  str(a)
            new_row = {
                    'nmr_kel':nmr_kel,
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
            for c,d in json_hhcw["table"][nmr_kel].items():
                new_row[c] = d
            df_suarapartai = df_suarapartai.append(new_row, ignore_index=True)

    ##finish
    panjang1=str(len(df_suarapartai.index))
    df_suarapartai["nmr_kel"]=df_suarapartai["nmr_kel"].astype(int)
    df_hasil = df_suarapartai.merge(df_daerah, on="nmr_kel", how="left")
    panjang2=str(len(df_hasil.index))
    df_hasil.to_csv("hasil/dprd_prov_hhcw_suarapartai/"+nmr_wilayah+" "+nama_wilayah+".csv")
    
    print("FINISH:"+str(datetime.datetime.now().strftime("%H:%M"))+" p:"+panjang1+" "+panjang2)

print("DONE kak")
