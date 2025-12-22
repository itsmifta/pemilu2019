import urllib.parse
import requests
import pandas as pd
import json
import csv
import datetime
import warnings

warnings.filterwarnings("ignore")

durl_kabupaten = 'https://pemilu2019.kpu.go.id/static/json/wilayah/' #+nomor wilayah
durl_calegpartai = 'https://pemilu2019.kpu.go.id/static/json/caleg/partai/' #+nomor dapil
durl_hr = 'https://pemilu2019.kpu.go.id/static/json/hr/dprri/' #+nomor dapil
dloc_daerah = 'hasil/daerah/daerah ' #+nomordapil+' '+namadapil+'.csv'

##buka file partai
json_partai = json.load(open('hasil/partai.json'))

##buka file dapil
df_dapil = pd.read_csv('hasil/dapildprri.csv', index_col=0)
df_dapil = df_dapil.transpose()

count1 = 0
er_calegpartai = 0
erlist_calegpartai = []
er_hr = 0
erlist_hr = []
er_hr_suara = 0
erlist_hr_suara = []
er_hr_kel = 0
erlist_hr_kel = []

for index, row in df_dapil.iterrows():
    nomordapil = str(index)
    namadapil = row['nama']
    wilayah = str(row['wilayah'][1:-1])
    print(nomordapil+' '+namadapil+' '+wilayah)
    print('WAKTU START:'+str(datetime.datetime.now()))

    ## ambil data calegpartai
    url_calegpartai = durl_calegpartai+nomordapil+'.json'
    try:
        json_calegpartai = requests.get(url_calegpartai, verify=False).json()
    except Exception as e:
        print('error calegpartai: '+str(e))
        er_calegpartai += 1
        erlist_calegpartai.append(nomordapil +str(e))
        row_er_calegpartai = namadapil+','+nomordapil+',calegpartai,dapil,'+nomordapil+','+str(e)+'\n'
        with open('hasil/dprri_hr/error_dprri_hr.csv','a', newline='\n') as fd:
            fd.write(row_er_calegpartai)
        
    df_calegpartai = pd.DataFrame(columns = ['nmr_partai','nama_partai','nmr_caleg','nama_caleg','jk'])
    for a, b in json_calegpartai.items():
        nmr_partai = str(a)
        nama_partai = json_partai[a]['nama']
        for c in b.items():
            nmr_caleg = str(c[0])
            nama_caleg = str(c[1]['nama'])
            jk = str(c[1]['jenis_kelamin'])
            new_row = {'nmr_partai':nmr_partai,'nama_partai':nama_partai,'nmr_caleg':nmr_caleg,'nama_caleg':nama_caleg,'jk':jk}
            df_calegpartai = df_calegpartai.append(new_row, ignore_index=True)
    
    ## ambil data daerah
    file_daerah = 'daerah '+nomordapil+' '+namadapil+'.csv'
    df_daerahkel = pd.read_csv('hasil/daerah/'+file_daerah, index_col=0)
    df_daerahkec = df_daerahkel[['nmr_kab','nama_kab','nmr_kec','nama_kec']].copy()
    df_daerahkec.drop_duplicates(inplace=True)
    df_daerahkec.reset_index(inplace=True, drop=True)
    
    df_suarapartai = df_daerahkel[['nmr_kel']].copy()

    df_hr = pd.DataFrame(columns = ['nmr_kel','nmr_caleg','suara'])
    
    ## ambil data hr
    nama_kab = ''
    for index, row in df_daerahkec.iterrows():
        nmr_kab = str(row['nmr_kab'])
        nama_kab_before = nama_kab
        nama_kab = str(row['nama_kab'])
        if nama_kab != nama_kab_before:       
            print(str(row['nama_kab']))
        nmr_kec = str(row['nmr_kec'])
        try:
            url_hr = durl_hr+nomordapil+'/'+nmr_kab+'/'+nmr_kec+'.json'
            json_hr = requests.get(url_hr, verify=False).json()
            try:
                for a, b in json_hr['table'].items():
                    nmr_kel = a
                    for c, d in json_hr['table'][a].items():
                        try:
                            nmr_caleg = str(c)
                            suara = str(d)
                            new_row = {'nmr_kel':nmr_kel,'nmr_caleg':nmr_caleg,'suara':suara}
                            df_hr = df_hr.append(new_row, ignore_index=True)
                        except Exception as e_hr_suara:
                            print('error hr suara: (kel:'+str(nmr_kel)+') '+str(e_hr_suara))
                            er_hr_suara +=1
                            erlist_hr_suara.append('kel:'+str(nmr_kel)+',error:'+str(e_hr_suara))
                            row_er_hr_suara = namadapil+','+nomordapil+',hrsuara,kel,'+str(nmr_kel)+','+str(e_hr_suara)+'\n'
                            with open('hasil/dprri_hr/error_dprri_hr.csv','a',newline='\n') as fd:
                                fd.write(row_er_hr_suara)
            except Exception as e_hr_kel:
                print('error hr load kel: (kec:'+str(nmr_kec)+') '+str(e_hr_kel))
                er_hr_kel +=1
                erlist_hr_kel.append('load kel, kec:'+str(nmr_kec)+',error:'+str(e_hr_kel))
                row_er_hr_kec = namadapil+','+nomordapil+',loadkel,kec,'+str(nmr_kec)+','+str(e_hr_kel)+'\n'
                with open('hasil/dprri_hr/error_dprri_hr.csv','a',newline='\n') as fd:
                    fd.write(row_er_hr_kec)
        except Exception as e_hr:
            print('error hr: (kec:'+str(nmr_kec)+') '+str(e_hr))
            er_hr += 1
            erlist_hr.append('kec:'+str(nmr_kec)+',error:'+str(e_hr))
            row_er_hr = namadapil+','+nomordapil+',hrkec,kec,'+str(nmr_kec)+','+str(e_hr)+'\n'
            with open('hasil/dprri_hr/error_dprri_hr.csv','a',newline='\n') as fd:
                fd.write(row_er_hr)
            
    ## finish
    df_dprri = pd.merge(df_calegpartai, df_hr, on=['nmr_caleg'])
    df_dprri['nmr_kel']=df_dprri['nmr_kel'].astype(int)
    df_dprri = pd.merge(df_daerahkel, df_dprri, on=['nmr_kel'])
    df_dprri.to_csv('hasil/dprri_hr/'+nomordapil+' '+namadapil+'.csv')
    
    print('WAKTU FINISH:'+str(datetime.datetime.now()))

with open("hasil/dprri_hr/error_auto.txt", "w") as output:
    output.write('hr: '+str(erlist_hr))
    output.write('hr_kel: '+str(erlist_hr_kel))
    output.write('hr_suara: '+str(erlist_hr_suara))
    output.write('calegpartai: '+str(erlist_calegpartai))
print('list error calegpartai: ('+str(er_calegpartai)+') '+str(erlist_calegpartai))
print('list error hr: ('+str(er_hr)+') '+str(erlist_hr))
print('list error hr load kel: ('+str(er_hr_kel)+') '+str(erlist_hr_kel))
print('list error hr suara: ('+str(er_hr_suara)+') '+str(erlist_hr_suara))
print("DONE kak")


