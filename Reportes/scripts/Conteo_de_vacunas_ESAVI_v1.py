import requests
import json
import pandas as pd

dhis2_auth = ('user', 'Passwork')
urlBase = "https://dominio_instancia.org/api/"

# Base URL para realizar las solicitudes a la API
url = urlBase + '38/analytics/events/query/aFGRl00bzio?dimension=ou%3AUSER_ORGUNIT%3BUSER_ORGUNIT_CHILDREN%3BUSER_ORGUNIT_GRANDCHILDREN,...'
url2 = urlBase + "29/categoryOptions"
url3 = urlBase + "29/options?fields=name,code&filter=optionSet.id:in:[PrAA7nJPXke,IQ7u8KsQfco]&filter=code:eq:"
url4 = urlBase + "29/categoryOptions?filter=name:ne:default&fields=id,name&filter=code:token:"
url5 = urlBase + "29/categories/X8f6OtfsPwJ.json"
url6 = urlBase + "29/categories"
url7 = urlBase + "38/maintenance?categoryOptionComboUpdate=true&cacheClear=true&appReload=true"
url9 = urlBase + "29/organisationUnits?fields=id&withinUserHierarchy=true&pageSize=1&query="
url10 = urlBase + "29/categoryOptionCombos?fields=id,name&filter=name:eq:"
url11 = urlBase + "dataValueSets.json?async=true&dryRun=false&strategy=NEW_AND_UPDATES&preheatCache=false&skipAudit=false&dataElementIdScheme=UID&orgUnitIdScheme=UID&idScheme=UID&skipExistingCheck=false&format=json"
url12 = urlBase + "system/taskSummaries/DATAVALUE_IMPORT/"

# Encabezados para las solicitudes HTTP
headers = {'Accept': 'application/json', "Content-Type": "application/json"}

def contar_coincidencias(data_rows):
    """
    Calcula las coincidencias de registros de vacunación en función de la edad y otros criterios.
    
    :param data_rows: Lista de filas de datos a procesar
    :return: Lista de coincidencias encontradas
    """
    list_data = []
    print("Calculando coincidencias...")
    
    # Crear DataFrame a partir de los datos
    df = pd.DataFrame(data_rows, columns=["Registro", "OU", "FechaNacimiento", "Genero", "nomVac1", ...])
    date_register = df['Registro']
    df['Registro'] = pd.to_datetime(df['Registro'])
    df['FechaNacimiento'] = pd.to_datetime(df['FechaNacimiento'])
    
    # Calcular edad y rango de edad
    df['DiferenciaDias'] = (df['Registro'] - df['FechaNacimiento']).dt.days
    df['Edad'] = df['DiferenciaDias'] / 365.25
    rangos_edad = [(-1, 0.999), (1, 17), (18, 24), (25, 49), (50, 59), (60, 69), (70, 79), (80, float('inf'))]
    labels = ['0-12 meses', '1-17 años', '18-24 años', '25-49 años', '50-59 años', '60-69 años', '70-79 años', '80 o más años']
    df['RangoEdad'] = pd.cut(df['Edad'], bins=[lim_inf for (lim_inf, lim_sup) in rangos_edad] + [float('inf')], labels=labels)
    
    print("Tabla de datos")
    df['Registro'] = date_register
    df = df[['Registro', 'OU', 'Genero', 'FechaNacimiento', 'Edad', 'RangoEdad', ...]]
    
    # Calcular diferencias y agrupar datos
    for index in range(8):
        df['fecEsavi'] = pd.to_datetime(df['fecEsavi'])
        df['fecvac' + str(index + 1)] = pd.to_datetime(df['fecvac' + str(index + 1)])
        df['DiferenciaDias'] = (df['fecEsavi'] - df['fecvac' + str(index + 1)]).dt.days
        
        # Rango de días desde la vacuna
        bins = [0, 30, 80, float('inf')]
        labels = ['0-30', '30-80', '80 o más']
        df['RangoDiasVacuna'] = pd.cut(df['DiferenciaDias'], bins=bins, labels=labels, right=False)
        
        # Agrupar por diferentes criterios y contar
        grupo_por_hospital = df.groupby(['Registro', 'OU', 'Genero', 'RangoEdad', 'RangoDiasVacuna', ...], observed=False).size().reset_index(name='Cantidad')
        json_data = grupo_por_hospital.to_json(orient='records')
        
        for data_export in json.loads(json_data):
            clave = 'nomVac' + str(index + 1)
            if data_export['Cantidad'] > 0 and data_export[clave] != "":
                print(data_export)
                list_data.append(data_export)
                
            if data_export['Cantidad'] > 0 and index == 0 and data_export[clave] == "":
                data_export[clave] = "Sin info"
                print(data_export)
                list_data.append(data_export)

    return list_data
    

def get_Data():
    """
    Consulta datos desde el servidor y procesa los resultados.
    """
    print("Consultando datos en el servidor")
    response = requests.get(url, auth=dhis2_auth)
    data_rows = json.loads(response.text)
    
    if data_rows.get('rows'):
        if len(data_rows) > 0:
            data_rows = data_rows['rows']
            get_categoryOptions(data_rows)
        else:
            print("No hay datos")
    else:
        print("Error al consultar los datos")
    

def get_categoryOptions(data_rows):
    """
    Obtiene las opciones de categoría y las actualiza según sea necesario.
    
    :param data_rows: Lista de filas de datos procesados
    """
    item_code = []
    
    # Filtrar para categorizar los inputs que están vacíos
    for valor_a_row in data_rows:
        for indice, valor in enumerate(valor_a_row):
            if indice >= 4:
                if valor != '':
                    if valor not in item_code:
                        if '00:00:00.0' not in valor and valor not in ['0', '1', '2', '3']:
                            item_code.append(valor)
            elif indice == 4 and valor == '':
                if "Sin info" not in item_code:
                    item_code.append('Sin info')
    
    response_categories = requests.get(url5, auth=dhis2_auth)
    Categoria_data = json.loads(response_categories.text)
    
    # Eliminación de atributos que no permiten la actualización de la categoría
    del Categoria_data['lastUpdated'], Categoria_data['href'], Categoria_data['created']
    creacion_Metadata(item_code, Categoria_data, data_rows)  # Llamada a la función que crea y actualiza los metadatos necesarios


def creacion_Metadata(item_code, Categoria_data, data_rows):
    """
    Crea y actualiza metadatos basados en las opciones de categoría encontradas.
    
    :param item_code: Lista de códigos de las opciones
    :param Categoria_data: Datos de la categoría a actualizar
    :param data_rows: Filas de datos procesados
    """
    print("Creación y actualización de Metadatos")
    print(len(item_code), " tipos de vacunas detectados")
    
    for value_options in item_code:
        # Consulta del nombre de las opciones del optionSet de vacunas
        response_options = requests.get(url3 + value_options, auth=dhis2_auth)
        name_options = json.loads(response_options.text)
        
        if len(name_options['options']) > 0:
            code_options = name_options['options'][0]['code']
            name_options = name_options['options'][0]['name']
            categoryOptions = {"code": code_options, "formName": name_options, "name": name_options, "organisationUnits": []}
            
            # Consulta de las opciones de la categoría
            response_categoryOptions = requests.get(url4 + value_options, auth=dhis2_auth)
            lista_categoryOptions = json.loads(response_categoryOptions.text)
            lent_categoryOptions = len(lista_categoryOptions['categoryOptions'])
            
            # Creación de opciones que no existen
            if lent_categoryOptions == 0:
                response_post_categoryOptions = requests.post(url2, data=json.dumps(categoryOptions), auth=dhis2_auth, headers=headers)  # Creación de opciones de categorías que no existían
                response_post_categoryOptions = json.loads(response_post_categoryOptions.text)
                Categoria_data['categoryOptions'].append({'id': response_post_categoryOptions['response']['uid']})
            else:
                verifications = any(lista_categoryOptions['categoryOptions'][0]['id'] == item['id'] for item in Categoria_data['categoryOptions'])
                if verifications:
                    print("La categoryOptions ya está en la lista.")
                else:
                    print("La categoryOption se agregó a la lista.")
                    Categoria_data['categoryOptions'].append({'id': lista_categoryOptions['categoryOptions'][0]['id']})
    
    print("Se actualizó las categorías de opciones")
    data_update = json.dumps(Categoria_data).replace("'", '"')
    response_update_Categoria = requests.put(url5, data=data_update, auth=dhis2_auth, headers=headers) 
    result_update(response_update_Categoria, data_rows)
    


def result_update(updateCategoria, data_rows):
    """
    Procesa los resultados de la actualización de categorías.
    
    :param updateCategoria: Respuesta de la actualización de categoría
    :param data_rows: Filas de datos procesados
    """
    if updateCategoria.status_code == 200:
        # updateCategoria_mantinimiento = requests.post(url7, auth=dhis2_auth, headers=headers)  # Actualización de opciones de categoría
        print(contar_coincidencias(data_rows))
    else:
        print("Falló el proceso de creación y actualización de metadatos")


# Limpieza de datos para cargar
def Precarga_datos_analiticos(data_analiticos):   
    """
    Limpia los datos antes de la carga en DHIS2.
    
    :param data_analiticos: Datos analíticos a procesar
    """
    print("Limpieza de datos")
    carga(data_analiticos, len(data_analiticos))


# Carga de datos a DHIS2, filtrando los datos
def carga(data_import, num_data):
    """
    Carga los datos importados en DHIS2.
    
    :param data_import: Datos a cargar
    :param num_data: Número de datos a cargar
    """
    num_import = num_data
    data_import_carga = []
    print("Carga de datos")
    
    for value_json in data_import:
        num_import -= 1
        
        # Consulta de OU
        get_id_OU = requests.get(url9 + value_json['OU'], auth=dhis2_auth)
        get_id_OU = json.loads(get_id_OU.text)  
        
        if len(get_id_OU['organisationUnits']) > 0:  # Verifica que existan organizaciones
            for index in range(6):  # Límite de campos
                if 'nomVac' + str(index + 1) in value_json:
                    get_options = requests.get(url3 + value_json['nomVac' + str(index + 1)], auth=dhis2_auth)
                    name_option = json.loads(get_options.text)
                    if len(name_option['options']) > 0:
                        vacunas = name_option['options'][0]['name']
                    else:
                        vacunas = None
            
            # Determina el género
            if value_json['Genero'] == '2':
                sex = 'Femenino'
            elif value_json['Genero'] == '1':
                sex = 'Masculino'
            elif value_json['Genero'] != '1' and value_json['Genero'] != '2':
                sex = 'Otro'

            # Determina si es grave
            grave = 'G-Sí' if value_json['Grave'] == '1' else ('G-No' if value_json['Grave'] == '0' else 'G-No sabe')

            # Determina si está embarazada
            Ispregnancy = 'E-Sí' if value_json['Ispregnancy'] == '1' else ('E-No sabe' if value_json['Ispregnancy'] in ['3', ''] else 'E-No')
            
            if vacunas is not None:
                co = f"{vacunas}, {sex}, {value_json['RangoEdad']}, {value_json['RangoDiasVacuna']}, {grave}, {Ispregnancy}"  # Crea la clave para la búsqueda
                get_co = requests.get(url10 + co, auth=dhis2_auth)  # Consulta para obtener el id de CO
                get_co = json.loads(get_co.text)

                date = str(value_json['Registro']).replace("-", "").replace(" 00:00:00.0", "")
                
                if len(get_co['categoryOptionCombos']) > 0:  # Verifica que la lista no esté vacía
                    # Construcción del objeto a cargar en DHIS2
                    data = {
                        "dataElement": "HeiP2JHGQ6R",  # Dato por defecto
                        "categoryOptionCombo": get_co['categoryOptionCombos'][0]['id'], 
                        "period": date,  # Periodo a registrar
                        "orgUnit": get_id_OU['organisationUnits'][0]['id'],
                        "value": value_json['Cantidad'],
                        "attributeOptionCombo": "HllvX50cXC0"
                    }
                    data_import_carga.append(data)

    postData = requests.post(url11, data=json.dumps({"dataValues": data_import_carga}), auth=dhis2_auth, headers=headers)  # Carga del objeto
    _data_postData = json.loads(postData.text)
    id_import = _data_postData['response']['id']
    
    if postData.status_code == 200:
        response_import = requests.get(url12 + id_import, auth=dhis2_auth)  # Consulta para verificar el estado
        

get_Data()  # Inicia la consulta de datos
