import requests
import json
import pandas as pd


dhis2_auth = ('user', 'Passwork')
urlBase = "https://dominio_instancia.org/api/"

url = urlBase+"38/analytics/events/query/aFGRl00bzio?dimension=ou%3AUSER_ORGUNIT%3BUSER_ORGUNIT_CHILDREN%3BUSER_ORGUNIT_GRANDCHILDREN,oindugucx72,NI0QRzJvQ0k,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&headers=eventdate,ouname,oindugucx72,NI0QRzJvQ0k,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&totalPages=false&eventDate=THIS_YEAR,LAST_5_YEARS&displayProperty=SHORTNAME&outputType=EVENT&includeMetadataDetails=true&stage=lSpdre0srBn&pageSize=500"
url2 = urlBase+"29/categoryOptions"
url3 = urlBase+"29/options?fields=name&filter=optionSet.id:in:[PrAA7nJPXke,IQ7u8KsQfco]&filter=code:eq:"
url4 = urlBase+"29/categoryOptions?filter=name:ne:default&fields=id,name&filter=identifiable:token:"
url5 = urlBase+"29/categories/X8f6OtfsPwJ.json"
url6 = urlBase+"29/categories"
url7 = urlBase+"38/maintenance?categoryOptionComboUpdate=true&cacheClear=true&appReload=true"
url9 = urlBase+"29/organisationUnits?fields=id&withinUserHierarchy=true&pageSize=1&query="
url10 = urlBase+"29/categoryOptionCombos?fields=id,name&filter=name:eq:"
url11 = urlBase+"dataValueSets.json?async=true&dryRun=false&strategy=NEW_AND_UPDATES&preheatCache=false&skipAudit=false&dataElementIdScheme=UID&orgUnitIdScheme=UID&idScheme=UID&skipExistingCheck=false&format=json"
url12 = urlBase+"system/taskSummaries/DATAVALUE_IMPORT/"

headers = {'Accept': 'application/json', "Content-Type": "application/json"}

def contar_coincidencias(data_rows):
    list_data=[]
    print("Calculando coincidencias...")
    df = pd.DataFrame(data_rows, columns=["Registro", "OU", "Genero","FechaNacimiento","Grave","Ispregnancy"])
    date_register=df['Registro']
    df['Registro'] = pd.to_datetime(df['Registro'])
    df['FechaNacimiento'] = pd.to_datetime(df['FechaNacimiento'])
    df['DiferenciaDias'] = (df['Registro'] - df['FechaNacimiento']).dt.days
    df['Edad'] = df['DiferenciaDias'] / 365.25
    rangos_edad = [(-1, 0.999), (1, 17), (18, 24), (25, 49), (50, 59), (60, 69), (70, 79), (80, float('inf'))]
    labels = ['0-12 meses', '1-17 años', '18-24 años', '25-49 años', '50-59 años', '60-69 años', '70-79 años', '80 o más años']
    df['RangoEdad'] = pd.cut(df['Edad'], bins=[lim_inf for (lim_inf, lim_sup) in rangos_edad] + [float('inf')], labels=labels)
    print("Tabla de datos")
    df['Registro']=date_register
    df=df[['Registro', 'OU', 'Genero', 'FechaNacimiento', 'Edad', 'RangoEdad',"Grave","Ispregnancy"]]
    grupo_por_hospital = df.groupby(['Registro','OU', 'Genero','RangoEdad',"Grave","Ispregnancy"]).size().reset_index(name='Cantidad')
    json_data = grupo_por_hospital.to_json(orient='records')
    for data_export in json.loads(json_data):
        if( data_export['Cantidad'] >= 1):
            list_data.append(data_export)   
    json_data = df.to_json()
    return(list_data)
   
def get_Data():
    print("Consultando datos en el servidor")
    response = requests.get(url, auth=dhis2_auth)     
    data_rows = json.loads(response.text)

    if len(data_rows)>0:
        data_rows=data_rows['rows']
        carga(contar_coincidencias(data_rows), len(contar_coincidencias(data_rows)))

    else:
        print("No hay datos")

    

# carga de datos a DHI2, filtrando los datos
def carga(data_import, num_data):
    num_import=num_data
    data_imporT_carga =[]
    num_auxi=num_data
    print("Carga de datos")
    for value_json in data_import:
        num_import = num_import-1
        # consulta de ou
        get_id_OU = requests.get(url9+value_json['OU'], auth=dhis2_auth)
        get_id_OU=json.loads(get_id_OU.text) 
        num_auxi=num_auxi-1
        if  len(get_id_OU['organisationUnits'])>0:
            if value_json['Genero'] == '2': # se selecciona el genero debido a las combinaciones de opciones de categoria
                sex ='Femenino'
            
            elif value_json['Genero'] == '1':
                sex ='Masculino'

            if value_json['Grave'] == '1':
                grave = 'G-Sí'
                
            elif value_json['Grave'] == '0':
                grave = 'G-No'

            elif value_json['Grave'] == '':
                grave = 'G-No sabe'
            
            if value_json['Ispregnancy'] == '1':
                Ispregnancy = 'E-Sí'

            elif value_json['Ispregnancy'] == '3' or value_json['Ispregnancy'] == '':
                Ispregnancy = 'E-No sabe'

            elif value_json['Ispregnancy'] == '2':
                Ispregnancy = 'E-No'
            co = "Eventos, "+sex+", "+value_json['RangoEdad']+", "+ grave +", "+ Ispregnancy # Se crea la palabla clave para la búsqueda
            get_co = requests.get(url10+co, auth=dhis2_auth) # se realiza la consulta para consultar el id de CO
            get_co=json.loads(get_co.text)
            date= str(value_json['Registro']).replace("-","").replace(" 00:00:00.0","")
            if len(get_co['categoryOptionCombos'])>0:# Si el tamaño de la lista es igual a 0 no debe de realizar el proceso
               # Construcion de objecto a cargar en DHIS2
                data={
                "dataElement": "NVihtmQcw9u", #dato por defecto
                "categoryOptionCombo": get_co['categoryOptionCombos'][0]['id'], 
                "period": date, # periodo a registrar en el DataSet
                "orgUnit": get_id_OU['organisationUnits'][0]['id'],
                "value": value_json['Cantidad'],
                "attributeOptionCombo": "HllvX50cXC0",
                }
                data_imporT_carga.append(data)
    postData = requests.post(url11,data=json.dumps({"dataValues": data_imporT_carga}), auth=dhis2_auth, headers=headers) # carga del objecto
    _data_postData=json.loads(postData.text)
    id_import=_data_postData['response']['id']
    if (postData.status_code==200):
        response_import = requests.get(url12+id_import, auth=dhis2_auth) 
        print(json.loads(response_import.text)) #status de proceso

get_Data()