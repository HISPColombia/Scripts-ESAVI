import requests
import json
import pandas as pd



dhis2_auth = ('user', 'Passwork')
urlBase = "https://dominio_instancia/api/"

url = urlBase+'38/analytics/events/query/aFGRl00bzio?dimension=ou%3AUSER_ORGUNIT%3BUSER_ORGUNIT_CHILDREN%3BUSER_ORGUNIT_GRANDCHILDREN,oindugucx72,NI0QRzJvQ0k,oindugucx72%3AIN%3A2%3B1%3B3,lSpdre0srBn.uSVcZzSM3zg,lSpdre0srBn.dOkuCjpD978,lSpdre0srBn.g9PjywVj2fs,lSpdre0srBn.VrzEutEnzSJ,lSpdre0srBn.menOXwIFZh5,lSpdre0srBn.f4WCAVwjHz0,lSpdre0srBn.OU5klvkk3SM,lSpdre0srBn.H3TKHMFIN6V,lSpdre0srBn.X3PxqaO5f9r,lSpdre0srBn.ysWILv7evq2,lSpdre0srBn.sNAzULL8qKr,lSpdre0srBn.U7GsOtvm5XJ,lSpdre0srBn.wmEHb59whXs,lSpdre0srBn.bNGwInf25MO,lSpdre0srBn.SAZugYdkOZK,lSpdre0srBn.z2I8yMDvyXA,lSpdre0srBn.LYariSd5cEq,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&headers=eventdate,ouname,NI0QRzJvQ0k,oindugucx72,lSpdre0srBn.uSVcZzSM3zg,lSpdre0srBn.dOkuCjpD978,lSpdre0srBn.g9PjywVj2fs,lSpdre0srBn.VrzEutEnzSJ,lSpdre0srBn.menOXwIFZh5,lSpdre0srBn.f4WCAVwjHz0,lSpdre0srBn.OU5klvkk3SM,lSpdre0srBn.H3TKHMFIN6V,lSpdre0srBn.X3PxqaO5f9r,lSpdre0srBn.ysWILv7evq2,lSpdre0srBn.sNAzULL8qKr,lSpdre0srBn.U7GsOtvm5XJ,lSpdre0srBn.wmEHb59whXs,lSpdre0srBn.bNGwInf25MO,lSpdre0srBn.SAZugYdkOZK,lSpdre0srBn.z2I8yMDvyXA,lSpdre0srBn.LYariSd5cEq,lSpdre0srBn.fq1c1A3EOX5,lSpdre0srBn.U19JzF3LjsS&totalPages=false&eventDate=THIS_YEAR,LAST_5_YEARS&displayProperty=SHORTNAME&outputType=EVENT&includeMetadataDetails=true&stage=lSpdre0srBn&pageSize=400'
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
    df = pd.DataFrame(data_rows, columns=["Registro", "OU", "FechaNacimiento","Genero","nomVac1","fecvac1", "nomVac2","fecvac2", "nomVac3","fecvac3", "nomVac4","fecvac4", "nomVac5","fecvac5", "nomVac6","fecvac6","nomVac7","fecvac7","nomVac8","fecvac8","fecEsavi","Grave","Ispregnancy"])
    date_register=df['Registro']
    df['Registro'] = pd.to_datetime(df['Registro'])
    df['FechaNacimiento'] = pd.to_datetime(df['FechaNacimiento'])
    df['DiferenciaDias'] = (df['Registro'] - df['FechaNacimiento']).dt.days
    df['Edad'] = df['DiferenciaDias'] / 365.25  # Tomando en cuenta años bisiestos
    
    rangos_edad = [(-1, 0.999), (1, 17), (18, 24), (25, 49), (50, 59), (60, 69), (70, 79), (80, float('inf'))]
    labels = ['0-12 meses', '1-17 años', '18-24 años', '25-49 años', '50-59 años', '60-69 años', '70-79 años', '80 o más años']
    df['RangoEdad'] = pd.cut(df['Edad'], bins=[lim_inf for (lim_inf, lim_sup) in rangos_edad] + [float('inf')], labels=labels)
    
    print("Tabla de datos")
    df['Registro']=date_register
    df=df[['Registro', 'OU', 'Genero', 'FechaNacimiento', 'Edad', 'RangoEdad',"fecEsavi", "nomVac1","fecvac1", "nomVac2","fecvac2", "nomVac3","fecvac3", "nomVac4","fecvac4", "nomVac5","fecvac5", "nomVac6","fecvac6","nomVac7","fecvac7","nomVac8","fecvac8","Grave","Ispregnancy"]]
    for index in range(8):
        df['fecEsavi'] = pd.to_datetime(df['fecEsavi'])
        df['fecvac'+str(index+1)] = pd.to_datetime(df['fecvac'+str(index+1)])
        df['DiferenciaDias'] = (df['fecEsavi'] - df['fecvac'+str(index+1)]).dt.days
        bins = [0, 30, 80, float('inf')]
        labels = ['0-30', '30-80','80 o más']
        df['RangoDiasVacuna'] = pd.cut(df['DiferenciaDias'], bins=bins, labels=labels, right=False)
        grupo_por_hospital = df.groupby(['Registro','OU', 'Genero','RangoEdad','RangoDiasVacuna','nomVac'+str(index+1),"Grave","Ispregnancy"]).size().reset_index(name='Cantidad')
        json_data = grupo_por_hospital.to_json(orient='records')
        for data_export in json.loads(json_data):
            if( data_export['Cantidad'] >= 1 and data_export['nomVac'+str(index+1)]!=""):
                list_data.append(data_export)
        json_data = df.to_json()
    return(list_data)
    

def get_Data():
    print("Consultando datos en el servidor")
    response = requests.get(url, auth=dhis2_auth)     
    data_rows = json.loads(response.text)
    if len(data_rows)>0:
        data_rows=data_rows['rows']
        get_categoryOptions(data_rows)
    else:
        print("No hay datos")
    

def get_categoryOptions(data_rows):
    item_code=[]
    # filtro para categorizar los inputs que estan vacios
    for valor_a_row in data_rows:
        for indice, valor in enumerate(valor_a_row):
            if indice >= 2:
                if valor != '':
                    if valor not in item_code:
                        if '00:00:00.0' not in valor and valor!='2' and valor!='1'and valor!='0' and valor!='3':
                            item_code.append(valor)
    
    response_categories = requests.get(url5, auth=dhis2_auth)
    Categoria_data = json.loads(response_categories.text)
    # Eliminacion de atributos de la respuesta que no permiten la actializacion de la categoria con las nuevas opciones de categoria
    del Categoria_data['lastUpdated'], Categoria_data['href'] ,Categoria_data['created']
    creacion_Metadata(item_code,Categoria_data,data_rows) #Llamada de la funcion que crea y actualiza los metadatos necesarios


def creacion_Metadata(item_code,Categoria_data,data_rows):
    print("Creacion y actualizacion de Metadatos")
    print(len(item_code), " tipos de vacunas detectados")
    for value_options in item_code:  
        # consulta de nombre de las opcines del optionSet de vacunas
        response_options = requests.get(url3+value_options, auth=dhis2_auth)
        name_options=json.loads(response_options.text)

        if len(name_options['options'])>0:
            name_options=name_options['options'][0]['name']
            categoryOptions={"code": value_options, "formName": name_options,"name": name_options, "organisationUnits": []}
          # Consulta de las opciones de la categoria
            response_categoryOptions = requests.get(url4+value_options, auth=dhis2_auth)
            lista_categoryOptions=json.loads(response_categoryOptions.text)
            lent_categoryOptions=len(lista_categoryOptions['categoryOptions'])
            # Creacion de las opciones que no existen
            if lent_categoryOptions == 0:
                response_post_categoryOptions = requests.post(url2, data=json.dumps(categoryOptions), auth=dhis2_auth, headers=headers)#Creacion de opciones de categorias que no existian
                response_post_categoryOptions=json.loads(response_post_categoryOptions.text)
                Categoria_data['categoryOptions'].append({'id': response_post_categoryOptions['response']['uid']})
            else:
                verifications = any(lista_categoryOptions['categoryOptions'][0]['id'] == item['id'] for item in Categoria_data['categoryOptions'])
                if verifications:
                    print("La categoryOptions ya está en la lista.")
                    
                else:
                    print("La categoryOption se agrego a la lista.")
                    Categoria_data['categoryOptions'].append({'id': lista_categoryOptions['categoryOptions'][0]['id']})
    
    print(" Se actualizo las categorias de opciones")
    data_update=json.dumps(Categoria_data).replace("'",'"')
    response_update_Categoria  = requests.put(url5, data=data_update, auth=dhis2_auth, headers=headers) 
    result_update(response_update_Categoria,data_rows)
    


def result_update(updateCategoria,data_rows):
    if updateCategoria.status_code == 200: 
        updateCategoria_mantinimiento = requests.post(url7, auth=dhis2_auth, headers=headers)#actualizacion de opciones de categoria en administracion de datos
        print(updateCategoria_mantinimiento, "Se realizo la creacion y actualizacion de la metadata del data Set")
        Precarga_datos_analiticos(contar_coincidencias(data_rows))

    else:
        print("fallo el proceso creacion y actualizacion de metadatos")


# limpieza de datos para cargar
def Precarga_datos_analiticos(data_analiticos):   
    print("limpieza de datos")
    carga(data_analiticos,len(data_analiticos))

# carga de datos a DHI2, filtrando los datos
def carga(data_import, num_data):
    num_import=num_data
    data_imporT_carga =[]
    print("Carga de datos")
    for value_json in data_import:
        num_import = num_import-1
        # consulta de ou
        get_id_OU = requests.get(url9+value_json['OU'], auth=dhis2_auth)
        get_id_OU=json.loads(get_id_OU.text)  
            
        if  len(get_id_OU['organisationUnits'])>0: #si es igual a 0 no debe de realizar el proceso
            for index in range(6): # El 6 es debido a que hay ese limite de campos
                if 'nomVac'+str(index+1) in value_json:
                    get_options = requests.get(url3+value_json['nomVac'+str(index+1)], auth=dhis2_auth)
                    name_option=json.loads(get_options.text)
                    vacunas = name_option['options'][0]['name']
            
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
            co = vacunas+", "+sex+", "+value_json['RangoEdad']+", "+value_json['RangoDiasVacuna']+", "+ grave +", "+ Ispregnancy # Se crea la palabla clave para la búsqueda
            get_co = requests.get(url10+co, auth=dhis2_auth) # se realiza la consulta para consultar el id de CO
            get_co=json.loads(get_co.text)
            date= str(value_json['Registro']).replace("-","").replace(" 00:00:00.0","")
            if len(get_co['categoryOptionCombos'])>0:# Si el tamaño de la lista es igual a 0 no debe de realizar el proceso
               # Construcion de objecto a cargar en DHIS2
                data={
                "dataElement": "HeiP2JHGQ6R", #dato por defecto
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
        response_import = requests.get(url12+id_import, auth=dhis2_auth) # se realiza la consulta para consultar el id de CO
        print(json.loads(response_import.text)) #status de proceso
        
get_Data()