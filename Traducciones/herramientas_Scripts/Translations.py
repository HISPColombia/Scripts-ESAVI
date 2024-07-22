###########################################################################
## (C) TOHOURI ROMAIN-ROLLAND
## 10/15/2018
#README:This Python program updates dhis2 translation using a csv files
############################################################################
import csv
import requests
import json

payload = None
response = None
dhis2_auth = ('user','passwork')
baseurl = 'https://dominio_instancia/api/29/'###url a trabajar

url = None
headers = {'Accept': 'application/json', "Content-Type": "application/json"}
dataError=[]
with open('dataTraducciones.csv', encoding='latin') as csvfile:
    reader = csv.DictReader(csvfile)
    dataseq=0
    for row in reader:
        if row['objectid']!=None:
            dataseq=dataseq+1  
            # # url = baseurl + row['classname'] + '/' + row['objectid'] +'/translations'   
            url = baseurl + row['classname'] + '/' + row['objectid']####para atualizar las traducciones
            if(row['valuesB']!=None):
                payload = {'property': row['property'], 'locale': row['locale'], 'value': ''.join([str(row['valuesA'])+","+str(row['valuesB'])])}####*****
                if(row['valuesC']!=None):
                    payload = {'property': row['property'], 'locale': row['locale'], 'value': ''.join([str(row['valuesA'])+","+str(row['valuesB'])+","+str(row['valuesC'])])}
                    if(row['valuesD']!=None):
                        payload = {'property': row['property'], 'locale': row['locale'], 'value': ''.join([str(row['valuesA'])+","+str(row['valuesB'])+","+str(row['valuesC'])+","+str(row['valuesD'])])}
                        if(row['valuesE']!=None):
                            payload = {'property': row['property'], 'locale': row['locale'], 'value': ''.join([str(row['valuesA'])+","+str(row['valuesB'])+","+str(row['valuesC'])+","+str(row['valuesD'])+","+str(row['valuesE'])])}
                            if(row['valuesF']!=None):
                                payload = {'property': row['property'], 'locale': row['locale'], 'value': ''.join([str(row['valuesA'])+","+str(row['valuesB'])+","+str(row['valuesC'])+","+str(row['valuesD'])+","+str(row['valuesE'])+","+str(row['valuesF'])])}
            else:
                payload = {'property': row['property'], 'locale': row['locale'], 'value': row['valuesA']}
            ###******** 1- get the dataElement object first
            response = requests.get(url, auth=dhis2_auth)
            print("GET response: ================================ \n" + response.text)        
            print("\n URL: " + url)
            if response.status_code == 200:                
                print("******"+str(dataseq),"******")
                data = json.loads(response.text)
                ###****** 2 - Appending the new translation to list of existing translations
                print("Existing Translation values : \n")
                print(json.dumps(data['translations'], sort_keys=True, indent=4))
                index=0                
                data['translations'].append(payload)
                for translationsAuxi in data['translations']:                
                    if translationsAuxi['property'] == payload['property']:
                        if translationsAuxi['locale'] == payload['locale']:
                            if index==0:###Solo entra cuando ya existe la propiedad con la misma locale para actualizar el value
                                translationsAuxi.update({'property':translationsAuxi['property'],'locale':translationsAuxi['locale'],'value':payload['value']})
                                index=index+1
                            else:
                                data['translations'].remove(translationsAuxi)
                print(index)
                print("\n New Translation values : \n")
                print(json.dumps(data['translations'], sort_keys=True, indent=4))
                print("\n URL: " + url)
                with open('jsonPAHOsucces.json', 'a') as file:
                    json.dump(data, file, indent=4)
                
                response = requests.put(url, data=json.dumps(data), auth=dhis2_auth, headers=headers)  #####para actualizar traducciones
                if response.status_code == 200:
                    print ("Entity correctly updated :" + response.text)
                else:
                    print("Error: Unable to update PUT record : ====================== \n" + response.text)
                    print(response.json)
                    print(response.status_code)
                    with open('jsonPAHOError.json', 'a') as file:
                        json.dump(row['objectid'], file, indent=4)

            else:                
                print("******"+str(dataseq),"******")
                with open('jsonPAHOError.json', 'a') as file:
                    json.dump(row['objectid'], file, indent=4)
                print("error: Unable to get data")
                print(response.text)
                print(response.status_code)
print(dataError)