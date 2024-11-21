import requests
import json
import pandas as pd
from datetime import datetime

dhis2_auth = ('user', 'Passwork')
urlBase = "https://dominio_instancia.org/api/"



url = urlBase+"38/analytics/events/query/aFGRl00bzio?dimension=ou%3AUSER_ORGUNIT%3BUSER_ORGUNIT_CHILDREN%3BUSER_ORGUNIT_GRANDCHILDREN,oindugucx72,NI0QRzJvQ0k,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&headers=eventdate,ouname,oindugucx72,NI0QRzJvQ0k,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&totalPages=false&eventDate=THIS_YEAR,LAST_5_YEARS&displayProperty=SHORTNAME&outputType=EVENT&includeMetadataDetails=true&stage=lSpdre0srBn&pageSize=10000"
url2 = urlBase+"29/categoryOptions"
url3 = urlBase+"29/options?fields=name&filter=optionSet.id:in:[PrAA7nJPXke,IQ7u8KsQfco]&filter=code:eq:"
url4 = urlBase+"29/categoryOptions?filter=name:ne:default&fields=id,name&filter=identifiable:token:"
url5 = urlBase+"29/categories/X8f6OtfsPwJ.json"
url6 = urlBase+"29/categories"
url9 = urlBase+"29/organisationUnits?fields=id&withinUserHierarchy=true&pageSize=1&query="
url10 = urlBase+"29/categoryOptionCombos?fields=id,name&filter=name:eq:"
url11 = urlBase+"dataValueSets.json?async=true&dryRun=false&strategy=NEW_AND_UPDATES&preheatCache=false&skipAudit=false&dataElementIdScheme=UID&orgUnitIdScheme=UID&idScheme=UID&skipExistingCheck=false&format=json"
url12 = urlBase+"system/taskSummaries/DATAVALUE_IMPORT/"

headers = {'Accept': 'application/json', "Content-Type": "application/json"}

def contar_coincidencias(data_rows):
    print("Calculando coincidencias...")
    df = pd.DataFrame(data_rows, columns=["Registro", "OU", "Genero","FechaNacimiento","Grave","Ispregnancy"])
    date_register=df['Registro']
    json_data=[]
    # Convertir la columna 'Fecha' a tipo datetime
    date_register = pd.to_datetime(df['Registro'])
    #Encontrar la fecha máxima y mínima
    if len(date_register)>0:
        fecha_maxima = date_register.max().strftime('%Y-%m-%d')
        fecha_minima = date_register.min().strftime('%Y-%m-%d')
        hospitales = df['OU'].drop_duplicates().tolist()
        json_data={"fecha_maxima":fecha_maxima,"fecha_minima":fecha_minima,"hospitales":str(len(hospitales))}
    else:
        print("No hay datos")
    return(json_data)

def get_Data():
    print("Consultando datos en el servidor")
    response = requests.get(url, auth=dhis2_auth)     
    data_rows = json.loads(response.text)
    if len(data_rows)>0:
        data_rows=data_rows['rows']
        if len(contar_coincidencias(data_rows))>0:
            carga(contar_coincidencias(data_rows))
        
    else:
        print("No hay datos")
    

# carga de datos a DHI2, filtrando los datos
def carga(data_import):
    print("Carga de datos")
    data={
        "dataElement": "NVihtmQcw9u", #dato por defecto
        "categoryOptionCombo": "H2R9gmcGZEI", 
        "period":  datetime.now().date().strftime("%Y-%m-%d"), # periodo a registrar en el DataSet
        "orgUnit": "hJHOsA7rdfs",#cambiar por la ou correspondiente
        "value": data_import,
        "attributeOptionCombo": "HllvX50cXC0",
        }
    postData = requests.post(url11,data=json.dumps({"dataValues": [data]}), auth=dhis2_auth, headers=headers) # carga del objecto
    _data_postData=json.loads(postData.text)
    id_import=_data_postData['response']['id']
    if (postData.status_code==200):
        response_import = requests.get(url12+id_import, auth=dhis2_auth) # se realiza la consulta para consultar el id de CO
        print(json.loads(response_import.text)) #status de proceso
        
get_Data()