##DPRD PROVINSI##
import urllib.parse
import requests
import pandas as pd
import json
import csv
import datetime
import warnings

warnings.filterwarnings("ignore")

durl_dapil = "https://pemilu2019.kpu.go.id/static/json/dapil/dprdprov/"#+nmr_wilayah
durl_ph = "https://pemilu2019.kpu.go.id/static/json/ph/"#+nmr_wilayah
durl_cp = "https://pemilu2019.kpu.go.id/static/json/caleg/partai/"#+nmr_dapil

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
        with open("hasil/dprd_prov_ph/error_ph.csv","a",newline="\n") as fd:
            fd.write(row_edapil)
        continue

    ##ambil data ph
    try:
        url_ph = durl_ph+nmr_wilayah+".json"
        json_ph = requests.get(url_ph, verify=False).json()
    except Exception as eph:
        print("error req ph ("+nmr_wilayah+nama_wilayah+") "+str(eph))
        row_eph = nmr_wilayah+","+nama_wilayah+",requestph,"+str(eph)+'\n'
        with open("hasil/dprd_prov_ph/error_ph.csv","a",newline="\n") as fd:
            fd.write(row_eph)
        continue
    
    df_ph = pd.DataFrame(columns = ["nmr_dapil","nmr_caleg","suara","rank"])
    df_cp = pd.DataFrame(columns = ['nmr_partai','nama_partai','nmr_caleg','nama_caleg','jk'])
    for a, b in json_ph["table"].items():
        nmr_dapil =  str(a)
        nama_dapil = json_dapil[a]['nama']
            
        ##ambil caleg partai
        try:
            url_cp = durl_cp+nmr_dapil+".json"
            json_cp = requests.get(url_cp, verify=False).json()
        except Exception as ecp:
            print("error cp ("+nmr_dapil+") "+str(ecp))
            row_ecp = nmr_wilayah+","+nama_wilayah+","+nmr_dapil+",requestph,"+str(ecp)+'\n'
            with open("hasil/dprd_prov_ph/error_cp.csv","a",newline="\n") as fd:
                fd.write(row_ecp)
            
        for a1, b1 in json_cp.items():
            nmr_partai = str(a1)
            nama_partai = json_partai[a1]['nama']
            for c1 in b1.items():
                nmr_caleg = str(c1[0])
                nama_caleg = str(c1[1]['nama'])
                jk = str(c1[1]['jenis_kelamin'])
                new_rowcp = {'nmr_partai':nmr_partai,'nama_partai':nama_partai,'nmr_caleg':nmr_caleg,'nama_caleg':nama_caleg,'jk':jk}
                df_cp = df_cp.append(new_rowcp, ignore_index=True)
            
        ##susun data ph suara caleg
        for c, d in json_ph["table"][nmr_dapil]["caleg"].items():
            nmr_caleg = str(c)
            suara = d["suara"]
            rank = d["ranking"]
            new_rowph = {"nmr_dapil":nmr_dapil,"nama_dapil":nama_dapil,"nmr_caleg":nmr_caleg,"suara":suara,"rank":rank}
            df_ph = df_ph.append(new_rowph, ignore_index=True)

    ##finisih
    panjang = str(len(df_ph.index))
    df_hasil = df_ph.merge(df_cp, on="nmr_caleg", how = "left")
    ## df_hasil = pd.merge(df_cp, df_ph, on=["nmr_caleg"])
    df_hasil.to_csv("hasil/dprd_prov_ph/"+nmr_wilayah+" "+nama_wilayah+".csv")
    print("FINISH:"+str(datetime.datetime.now().strftime("%H:%M"))+" ("+panjang+"rows)")

print("DONE kak")    
            
            
            
