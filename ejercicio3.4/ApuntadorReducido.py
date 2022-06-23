"""
Comandos iniciales en cmd/terminal:

----------Mongoimport----------
mongoimport --db test --collection tweets --drop --file=crisis.20190410.json

----------world----------
todos los archivos del zip de world, trabajopractico3
"""
import time
import datetime
import re
import psycopg2
import pandas as pd
import geopandas as gpd
from pymongo import MongoClient

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
MONGO_COLLECTION_NOMBRE2 = "muestraDeTweets"
POSTGRESQL_DB_NOMBRE = "world"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_HOST = "localhost"
DESCONOCIDO = "Unknown"
CONSTANTE_PORCENTAJE = 100

PATH_WORLD = r'.\world\ne_10m_admin_0_countries.shp'

#Mongo conexion
client = MongoClient(MONGO_HOST, MONGO_PUERTO)

database = client[MONGO_DB_NOMBRE]
collection = database[MONGO_COLLECTION_NOMBRE]
#collection = database[MONGO_COLLECTION_NOMBRE2]

#postgresql conexion
try:
    conexion = psycopg2.connect(
            database = POSTGRESQL_DB_NOMBRE,
            user = POSTGRESQL_USER,
            password = POSTGRESQL_PASSWORD,
            host = POSTGRESQL_HOST)

    #apuntador para crear diccionario Codigo/Ciudad:
    apuntador = conexion.cursor()

    #queryAPostgresql
    queryCountry = "SELECT NAME FROM COUNTRY"

    queryCityNombreCodigo = """
    SELECT 
        DISTINCT NAME, 
        COUNTRYCODE 
    FROM 
        CITY
    ORDER BY
        NAME,
        COUNTRYCODE;"""


    queryCityDistritoCodigo = """
    SELECT 
 	    DISTINCT DISTRICT, 
 	    COUNTRYCODE 
    FROM 
 	    CITY
    WHERE
	    DISTRICT != '' AND DISTRICT != ''
    ORDER BY
 	    DISTRICT,
 	    COUNTRYCODE;"""

    #diccionarios para renombrar las columnas
    countryColumnDict = {
        'name': 'paisNombre'
    }

    #query
    dfWorldCountry = pd.read_sql(queryCountry, conexion)

    #ciudadNombre,codigo
    apuntador.execute(queryCityNombreCodigo)
    listaCiudadCodigo = apuntador.fetchall()

    #distrito, codigo
    apuntador.execute(queryCityDistritoCodigo)
    listaDistritoCodigo = apuntador.fetchall()

    #renombrar
    dfWorldCountry = dfWorldCountry.rename(columns = countryColumnDict)

    #crear diccionarios:
    diccionarioCodigoCiudadNombre = {}
    diccionarioCodigoDistrito = {}

    #llenar diccionarios:
    #diccionarioCodigoCiudad
    for (ciudad, codigo) in listaCiudadCodigo:
        if not codigo in diccionarioCodigoCiudadNombre:
            diccionarioCodigoCiudadNombre[codigo] = [ciudad]
        else:
            diccionarioCodigoCiudadNombre[codigo].append(ciudad)

    #diccionarioCodigoDistrito
    for (distrito, codigo) in listaDistritoCodigo:
        if not codigo in diccionarioCodigoDistrito:
            diccionarioCodigoDistrito[codigo] = [distrito]
        else:
            diccionarioCodigoDistrito[codigo].append(distrito)

# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    print("Error al obtener la informacion", error)

# cerrar conexion
finally:
    if conexion:
        conexion.close()
        apuntador.close()
        print("Conexion con PostgreSQL cerrada")

#Punto2: geopandas dataframe (mapas de vuelta)
gpdworld = gpd.read_file(PATH_WORLD)

#re.escape para evitar caracter espaciales abiertos
def compararStringSimilar(document, elemento, esListaDeCiudades):
    stringDocumento = document["user"]["location"]
    if esListaDeCiudades:
        ciudadDocumento = stringDocumento.split(",")[0]
        return re.search(re.escape(elemento), ciudadDocumento, re.IGNORECASE)
    return re.search(re.escape(elemento), stringDocumento, re.IGNORECASE)

def compararStringSimilarLista(document, lista, esListaDeCiudades):
    listaDeVerdad = []
    if esListaDeCiudades:
        for elemento in lista:
            listaDeVerdad.append(bool(compararStringSimilar(document, elemento, esListaDeCiudades)))
    for elemento in lista:
        listaDeVerdad.append(bool(compararStringSimilar(document, elemento, esListaDeCiudades)))
    return any(listaDeVerdad)

def locationEstaVacio(document):
    return document["user"]["location"] == None

def nuevoValor(valor):
    return {"$set": {"user.country": valor}}

#examinar documentos con datos de world:
documentosAnalizados = 1
contadorUserLocationVacio = 0
contadorCoincidenciaPorPais = 0
contadorCoincidenciaPorCiudad = 0
contadorCoincidenciaPorDistrito = 0
contadorDocumentosSinCoincidencias = 0
totalDeDocumentos = len(list(collection.find({}, {'_id': 1, 'user.location': 1})))

def calcularPorcentajeSobreTotal(valor):
    return (valor / totalDeDocumentos) * CONSTANTE_PORCENTAJE

inicio = time.time()
for document in collection.find({},{'_id': 1, 'user.location': 1}):
    filtro = {'_id': document['_id']}
    PAIS_NOMBRE = 'paisNombre'
    if locationEstaVacio(document):
        #collection.update_one(filtro, nuevoValor(DESCONOCIDO))
        contadorUserLocationVacio += 1
    else:
        coincidenciaPorPais = False
        coincidenciaPorCiudad = False
        coincidenciaPorDistrito = False
        esListaDeCiudades = False
        for elemento in dfWorldCountry[PAIS_NOMBRE]:
            if (compararStringSimilar(document, elemento, esListaDeCiudades)):
                #collection.update_one(filtro, nuevoValor(elemento))
                coincidenciaPorPais = True
                contadorCoincidenciaPorPais += 1
                break
        if not coincidenciaPorPais:
            esListaDeCiudades = True
            for (codigoPais, listaDeCiudades) in diccionarioCodigoCiudadNombre.items():
                if(compararStringSimilarLista(document, listaDeCiudades, esListaDeCiudades)):
                    #collection.update_one(filtro, nuevoValor(codigoPais))
                    coincidenciaPorCiudad = True
                    contadorCoincidenciaPorCiudad += 1
                    break
        if not(coincidenciaPorCiudad | coincidenciaPorPais):
            esListaDeCiudades = False
            for (codigoPais, listaDeDistritos) in diccionarioCodigoDistrito.items():
                if(compararStringSimilarLista(document, listaDeDistritos, esListaDeCiudades)):
                    #collection.update_one(filtro, nuevoValor(codigoPais))
                    coincidenciaPorDistrito = True
                    contadorCoincidenciaPorDistrito += 1
                    break
        if not(coincidenciaPorDistrito | coincidenciaPorCiudad | coincidenciaPorPais):
            contadorDocumentosSinCoincidencias += 1
    paso = time.time()
    print("Documentos Analizados/Total: {}/{} | Progreso: {:.5f}% | Tiempo: {:.2f}s".format(documentosAnalizados, totalDeDocumentos, calcularPorcentajeSobreTotal(documentosAnalizados), paso - inicio))
    documentosAnalizados+= 1

#calculos finales:
#con vacios
sumaDeAnalizadosConVacios = contadorCoincidenciaPorPais + contadorCoincidenciaPorCiudad + contadorCoincidenciaPorDistrito + contadorUserLocationVacio

#sin vacios
sumaDeAnalizadosSinVacios = sumaDeAnalizadosConVacios - contadorUserLocationVacio

#porcentajes:
porcentajeUserLocationVacio = calcularPorcentajeSobreTotal(contadorUserLocationVacio)
porcentajePorPais = calcularPorcentajeSobreTotal(contadorCoincidenciaPorPais)
porcentajePorCiudad = calcularPorcentajeSobreTotal(contadorCoincidenciaPorCiudad)
porcentajePorDistrito = calcularPorcentajeSobreTotal(contadorCoincidenciaPorDistrito)
porcentajeConVacios = calcularPorcentajeSobreTotal(sumaDeAnalizadosConVacios)
porcentajeSinVacios = calcularPorcentajeSobreTotal(sumaDeAnalizadosSinVacios)
porcentajeDocumentosSinCoincidencias = calcularPorcentajeSobreTotal(contadorDocumentosSinCoincidencias)

#informacion
print("----------Finalizado----------")
print("Tiempo total de ejecucion: {}".format(datetime.timedelta(seconds=time.time()-inicio)))
print("Cantidad total de documentos: {}".format(totalDeDocumentos))
print("Documentos sin coincidencias: {} | Porcentaje: {:.2f}%".format(contadorDocumentosSinCoincidencias, porcentajeDocumentosSinCoincidencias))
print("Documentos con user.location vacio: {} | Porcentaje: {:.2f}%".format(contadorUserLocationVacio, porcentajeUserLocationVacio))
print("Documentos por pais: {} | Porcentaje: {:.2f}%".format(contadorCoincidenciaPorPais, porcentajePorPais))
print("Documentos por ciudad: {} | Porcentaje: {:.2f}%".format(contadorCoincidenciaPorCiudad, porcentajePorCiudad))
print("Documentos por distrito: {} | Porcentaje: {:.2f}%".format(contadorCoincidenciaPorDistrito, porcentajePorDistrito))
print("Analizados con vacios: {} | Porcentaje: {:.2f}%".format(sumaDeAnalizadosConVacios, porcentajeConVacios))
print("Analizados sin vacios: {} | Porcentaje: {:.2f}%".format(sumaDeAnalizadosSinVacios, porcentajeSinVacios))
print("*'vacios' son documentos que tenian user.location con valor nulo.")
print("**'sin coincidencias' son documentos que no coincidieron con ningun criterio de busqueda existente.")