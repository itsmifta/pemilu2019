import urllib.parse
import requests
import pandas as pd
import json
import csv
import datetime
import warnings

warnings.filterwarnings("ignore")

##buka file partai
json_partai = json.load(open('hasil/partai.json'))

##buka file dapil
df_wilayah = pd.read_csv('hasil/wilayah.csv')
df_wilayah = df_wilayah[['nmr_wilayah','nama_wilayah']].copy()
df_wilayah.drop_duplicates(inplace=True)
df_wilayah.reset_index(inplace=True, drop=True)
print(df_wilayah)

durl_hhcw = 'https://pemilu2019.kpu.go.id/static/json/hhcw/dprri/'

for index, row in df_wilayah.iterrows():
    nmr_wilayah = str(row['nmr_wilayah'][1:-1])
    nama_wilayah = row['nama_wilayah']

    if nmr_wilayah == '-99':
        continue

    print(nmr_wilayah+','+nama_wilayah+' START:'+str(datetime.datetime.now()))

    ##ambil data daerah
    df_daerah = pd.read_csv('hasil/wilayah/'+nmr_wilayah+' '+nama_wilayah+'.csv', index_col=0)
    column = [
        'nmr_kab',
        'nama_kab',
        'nmr_kec',
        'nama_kec',
        'nmr_kel',
        'nama_kel',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        '10',
        '11',
        '12',
        '13',
        '14',
        '15',
        '16',
        '17',
        '18',
        '19',
        '20'
        ]
    df_suarapartai = pd.DataFrame(columns=column)

    nama_kab = ''
    for index2, row2 in df_daerah.iterrows():
        nama_kab_before = nama_kab
        nmr_kab = str(row2['nmr_kab'])
        nama_kab = row2['nama_kab']
        if nama_kab != nama_kab_before:
            print(nama_kab)
        nmr_kec = str(row2['nmr_kec'])
        nama_kec = str(row2['nama_kec'])
        nmr_kel = str(row2['nmr_kel'])
        nama_kel = str(row2['nama_kel'])

        try:
            url_hhcw = durl_hhcw+nmr_wilayah+'/'+nmr_kab+'/'+nmr_kec+'/'+nmr_kel+'.json'
            json_hr = requests.get(url_hhcw, verify=False).json()
            new_row = {
                'nmr_kab':nmr_kab,
                'nama_kab':nama_kab,
                'nmr_kec':nmr_kec,
                'nama_kec':nama_kec,
                'nmr_kel':nmr_kel,
                'nama_kel':nama_kel,
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
            for a, b in json_partai.items():
                try:
                    new_row[a] = json_hr['chart'][a]
                except Exception as e:
                    continue
            try:
                df_suarapartai = df_suarapartai.append(new_row, ignore_index=True)
            except Exception as e_nr:
                print('gagal append new row '+nmr_kel)
                er_newrow = nmr_wilayah+','+nama_wilayah+','+nmr_kab+','+nmr_kec+','+nmr_kel+','+str(e_nr)+'\n'
                with open('hasil/dprri_suarapartai_hhcw/error_dprri_appendrow.csv','a+',newline='\n') as fd:
                    fd.write(er_newrow)
        except Exception as e_hhcw:
            print('erloadhhcw:'+str(e_hhcw)+','+url_hhcw)
            row_er_hhcw = nmr_wilayah+','+nama_wilayah+','+nmr_kab+','+nmr_kec+','+nmr_kel+','+str(e_hhcw)+'\n'
            with open('hasil/dprri_suarapartai_hhcw/error_dprri_loadhrkel.csv','a+',newline='\n') as fd:
                fd.write(row_er_hhcw)
    ## finish
    df_suarapartai.to_csv('hasil/dprri_suarapartai_hhcw/'+nmr_wilayah+' '+nama_wilayah+'.csv')
    print('WAKTU FINISH:'+str(datetime.datetime.now()))
    
print("DONE kak")
