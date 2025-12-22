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
durl_kabupaten = 'https://pemilu2019.kpu.go.id/static/json/wilayah/' #+nomor wilayah
durl_hr = 'https://pemilu2019.kpu.go.id/static/json/hr/dprri/' #+nomor dapil

##buka file dapil
df_dapil = pd.read_csv('hasil/dapildprri.csv', index_col=0)
df_dapil = df_dapil.transpose()
##buka file partai
json_partai = json.load(open('hasil/partai.json'))

count1 = 0
er_hrkel = 0
erlist_hrkel = []
er_suarapartai = 0
erlist_suarapartai = []
for index, row in df_dapil.iterrows():
    nomordapil = str(index)
    namadapil = row['nama']
    wilayah = str(row['wilayah'][1:-1])
    print(nomordapil+' '+namadapil+' '+wilayah)
    print('WAKTU START:'+str(datetime.datetime.now()))

    ## ambil data daerah
    file_daerah = 'daerah '+nomordapil+' '+namadapil+'.csv'
    df_daerah = pd.read_csv('hasil/daerah/'+file_daerah, index_col=0)
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
        nama_kab = str(row2['nama_kab'])
        if nama_kab != nama_kab_before:       
            print(nama_kab)
        nmr_kec = str(row2['nmr_kec'])
        nama_kec = str(row2['nama_kec'])
        nmr_kel = str(row2['nmr_kel'])
        nama_kel = str(row2['nama_kel'])
        try:
            url_hr = durl_hr+nomordapil+'/'+nmr_kab+'/'+nmr_kec+'/'+nmr_kel+'.json'
            json_hr = requests.get(url_hr, verify=False).json()
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
                    '''
                    print('error suara partai partai('+str(a)+'):'+str(e))
                    row_er_suarapartai = namadapil+','+nomordapil+','+nmr_kab+','+nmr_kec+','+nmr_kel+','+str(a)+','+str(e)+'\n'
                    with open('hasil/dprri_suarapartai/error_dprri_suarapartai.csv','a',newline='\n') as fd:
                        fd.write(row_er_suarapartai)
                    '''
            try:
                df_suarapartai = df_suarapartai.append(new_row, ignore_index=True)
            except Exception as e_nr:
                print('gagal append new row '+nmr_kel)
                er_newrow = namadapil+','+nomordapil+','+nmr_kab+','+nmr_kec+','+nmr_kel+','+str(e_nr)+'\n'
                with open('hasil/dprri_suarapartai/error_dprri_appendrow2.csv','a+',newline='\n') as fd:
                    fd.write(er_newrow)
            #print('.')
        except Exception as e_hr:
            print('erloadhr:'+str(e_hr)+','+url_hr)
            er_hrkel += 1
            erlist_hrkel.append(url_hr+','+str(e_hr))
            row_er_hrkel = namadapil+','+nomordapil+','+nmr_kab+','+nmr_kec+','+nmr_kel+','+str(e_hr)+'\n'
            with open('hasil/dprri_suarapartai/error_dprri_loadhrkel2.csv','a+',newline='\n') as fd:
                fd.write(row_er_hrkel)
    
    ## finish
    df_suarapartai.to_csv('hasil/dprri_suarapartai/'+nomordapil+' '+namadapil+'.csv')
    print('WAKTU FINISH:'+str(datetime.datetime.now()))

print('list error hrkel: ('+str(er_hrkel)+') '+str(erlist_hrkel))
print("DONE kak")
