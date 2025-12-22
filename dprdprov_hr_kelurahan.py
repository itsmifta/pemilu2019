import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import json
import csv
import datetime
import warnings

warnings.filterwarnings("ignore")

durl_cp = "https://pemilu2019.kpu.go.id/static/json/caleg/partai/"#+dapil
durl_dapil = "https://pemilu2019.kpu.go.id/static/json/dapil/dprdprov/" #+wil
durl_wil = "https://pemilu2019.kpu.go.id/static/json/wilayah/"
durl_kec = "https://pemilu2019.kpu.go.id/static/json/wilayah/"#+wil+kab(dapet dr atas)
durl_hr = "https://pemilu2019.kpu.go.id/static/json/hr/dprdprov/" #+wil+dapil+kab+kec

##buka file partai
json_partai = json.load(open('hasil/partai.json'))

##buka file wilayah
df_wilayah = pd.read_csv("hasil/wilayah_unik.csv", index_col=0)
print(df_wilayah)

#def retry
def requests_retry_session(
    retries=4,
    backoff_factor=0.3,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


for index, wil in df_wilayah.iterrows():
    nmr_wilayah = str(wil["nmr_wilayah"])
    nama_wilayah = wil["nama_wilayah"]
    print(nmr_wilayah+" "+nama_wilayah+" START "+str(datetime.datetime.now().strftime("%H:%M")))

    #csv daerah
    df_daerah = pd.read_csv("hasil/daerah_perwilayah/"+nmr_wilayah+" "+nama_wilayah+".csv", index_col=0)
    df_daerah = df_daerah.drop(df_daerah.columns[[0]], axis=1)

    #dapil
    url_dapil = durl_dapil+nmr_wilayah+".json"
    try:
        json_dapil = requests_retry_session().get(url_dapil, timeout=30, verify=False).json()
    #except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as err:
        
    except Exception as edapil:
        print("error req dapil (wil "+nama_wilayah+") "+str(edapil))
        row_edapil = nmr_wilayah+","+nama_wilayah+","+str(edapil)+"\n"
        with open("hasil/2dprd_prov_HR_caleg_kelurahan/error_dapil.csv","a",newline="\n") as fd:
            fd.write(row_edapil)
        continue

    df_cp = pd.DataFrame(columns = ['nmr_partai','nama_partai','nmr_caleg','nama_caleg','jk'])
    df_hr = pd.DataFrame(columns = ["nmr_wilayah","nama_wilayah","nmr_dapil","nama_dapil","nmr_kel","nmr_caleg","suara"])
    for index2, row2 in json_dapil.items():
        nmr_dapil = str(index2)
        nama_dapil = row2["nama"]
        print(nama_dapil+" "+str(datetime.datetime.now().strftime("%H:%M")))
        #kabupaten
        kab_list = row2["wilayah"]

        #calegpartai
        url_cp = durl_cp+nmr_dapil+".json"
        try:
            json_cp = requests_retry_session().get(url_cp, timeout=30, verify=False).json()
        except Exception as ecp:
            print("error req cp (dapil "+nama_dapil+") "+str(ecp))
            row_ecp = nmr_wilayah+","+nama_wilayah+","+nmr_dapil+","+nama_dapil+","+str(ecp)+"\n"
            with open("hasil/2dprd_prov_HR_caleg_kelurahan/error_cp.csv","a",newline="\n") as fd:
                fd.write(row_ecp)
            continue
        for a, b in json_cp.items():
            nmr_partai = str(a)
            nama_partai = json_partai[a]['nama']
            for c in b.items():
                nmr_caleg = str(c[0])
                nama_caleg = str(c[1]['nama'])
                jk = str(c[1]['jenis_kelamin'])
                new_row = {'nmr_partai':nmr_partai,'nama_partai':nama_partai,'nmr_caleg':nmr_caleg,'nama_caleg':nama_caleg,'jk':jk}
                df_cp = df_cp.append(new_row, ignore_index=True)

        #dapetin kecamatan
        for nmr_kab in kab_list:
            nmr_kab = str(nmr_kab)
            print("kab: "+nmr_kab+" "+str(datetime.datetime.now().strftime("%H:%M")))
            url_kab = durl_wil+nmr_wilayah+"/"+nmr_kab+".json"
            try:
                json_kec = requests_retry_session().get(url_kab, timeout=30, verify=False).json()
            except Exception as ekec:
                print("error req kec (kab "+nmr_kab+") "+str(ekec))
                row_ekec = nmr_wilayah+","+nama_wilayah+","+nmr_dapil+","+nama_dapil+","+nmr_kab+","+str(ekec)+"\n"
                with open("hasil/2dprd_prov_HR_caleg_kelurahan/error_req_kec.csv","a",newline="\n") as fd:
                    fd.write(row_ekec)
                continue

            #dapetin HR per kelurahan
            for index3, row3 in json_kec.items():
                nmr_kec = str(index3)
                nama_kec = row3["nama"]
                #print(nama_kec+" "+str(datetime.datetime.now().strftime("%H:%M")))
                url_hr = durl_hr+nmr_wilayah+"/"+nmr_dapil+"/"+nmr_kab+"/"+nmr_kec+".json"
                try:
                    json_hr = requests_retry_session().get(url_hr, timeout=30, verify=False).json()
                except Exception as ehr:
                    print("error req HR (kec "+nmr_kec+") "+str(ehr))
                    row_ehr = nmr_wilayah+","+nama_wilayah+","+nmr_dapil+","+nama_dapil+","+nmr_kab+","+nmr_kec+","+nama_kec+","+"req hr"+","+str(ehr)+"\n"
                    with open("hasil/2dprd_prov_HR_caleg_kelurahan/error_hr.csv","a",newline="\n") as fd:
                        fd.write(row_ehr)
                    continue
                
                #tulis hr
                try:
                    for index4, row4 in json_hr["table"].items():
                        nmr_kel =  str(index4)
                        for index5, row5 in row4.items():
                            nmr_caleg = str(index5)
                            suara = str(row5)
                            new_row = {"nmr_wilayah":nmr_wilayah,"nama_wilayah":nama_wilayah,"nmr_dapil":nmr_dapil,"nama_dapil":nama_dapil,"nmr_kel":nmr_kel,"nmr_caleg":nmr_caleg,"suara":suara}
                            df_hr = df_hr.append(new_row, ignore_index=True)
                except Exception as ejson_hr:
                    print("error json hr"+str(ejson_hr))
                    row_ejson_hr = nmr_wilayah+","+nama_wilayah+","+nmr_dapil+","+nama_dapil+","+nmr_kab+","+nmr_kec+","+nama_kec+","+"json hr"+","+str(ejson_hr)+"\n"
                    with open("hasil/2dprd_prov_HR_caleg_kelurahan/error_hr.csv","a",newline="\n") as fd:
                        fd.write(row_ejson_hr)
                    continue
    #finish
    df_hasil = df_hr.merge(df_cp, on="nmr_caleg", how="left")
    df_hasil["nmr_kel"]=df_hasil["nmr_kel"].astype(int)
    df_hasil = df_hasil.merge(df_daerah, on="nmr_kel", how="left")
    df_hasil.to_csv("hasil/2dprd_prov_HR_caleg_kelurahan/"+nmr_wilayah+" "+nama_wilayah+".csv")

print("DONE kak")
