"""
Comandos iniciales en cmd/terminal:

----------Mongoimport----------
mongoimport --db test --collection tweets --drop --file=crisis.20190410.json

----------world----------
todos los archivos del zip de world, trabajopractico3
"""
import time
import re
import psycopg2
import pandas as pd
import geopandas as gpd
import numpy as np
from pymongo import MongoClient
from pandas import Series, DataFrame
from functools import reduce
from collections import Counter

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

    # queryAPostgresql
    queryCountry = "SELECT CODE, CODE2, NAME, LOCALNAME FROM COUNTRY"
    queryCity = "SELECT COUNTRYCODE, DISTRICT, NAME FROM CITY"

    #diccionarios para renombrar las columnas
    countryColumnDict = {
        'code': 'codigo',
        'code2': 'codigoInternet',
        'name': 'paisNombre',
        'localname': 'paisNombreLocal'
    }
    cityColumnDict = {
        'countrycode': 'codigo',
        'district': 'distrito',
        'name': 'ciudadNombre'
    }
    #query
    dfWorldCountry = pd.read_sql(queryCountry, conexion)
    dfWorldCity = pd.read_sql(queryCity, conexion)

    #renombrar
    dfWorldCountry = dfWorldCountry.rename(columns = countryColumnDict)
    dfWorldCity = dfWorldCity.rename(columns = cityColumnDict)

# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    print("Error al obtener la informacion", error)

# cerrar conexion
finally:
    if conexion:
        conexion.close()
        print("Conexion con PostgreSQL cerrada")

#Punto2: geopandas dataframe (mapas de vuelta)
gpdworld = gpd.read_file(PATH_WORLD)

#mas datos para buscar
dfWorldExtras = gpdworld[['ADM0_A3', 'ABBREV', 'FORMAL_EN']]

#diccionario para renombrar las columnas
extrasColumnDict = {
    'ADM0_A3': 'codigo',
    'ABBREV': 'abreviatura',
    'FORMAL_EN': 'paisNombreFormal'
}

#renombrar
dfWorldExtras = dfWorldExtras.rename(columns = extrasColumnDict)

#merge: solo un dataframe para hacer la busqueda
dfApuntadorDePais = reduce(
    lambda left, right:
    pd.merge(left, right, on = ['codigo']),
    [dfWorldCountry, dfWorldCity, dfWorldExtras]
)

#conocer cuales seran los diferentes criterios
#todosLosLocations = collection.find(
#    {},
#    {"_id": 0, "user.location": 1}).distinct("user.location")
#seriesLocations = Series(todosLosLocations)

#re.escape para evitar caracter espaciales abiertos
def compararStringSimilarDocument_Dataframe(INDEX, document, elemento):
    return re.search(re.escape(str(dfApuntadorDePais[elemento][INDEX])), str(document["user"]["location"]),
                     re.IGNORECASE)

def locationNoEstaVacio(document):
    return document["user"]["location"] != None

def buscadorDeCoincidencias(INDEX, PAIS_NOMBRE, contadorDeAciertos, document, elemento):
    while (INDEX < len(dfApuntadorDePais[elemento])):
        if (compararStringSimilarDocument_Dataframe(INDEX, document, elemento)):
            contadorDeAciertos.append(dfApuntadorDePais[PAIS_NOMBRE][INDEX])
            INDEX += 1
        INDEX += 1

def revisarAcierto(document):
    contadorDeAciertos = []
    PAIS_NOMBRE = 'paisNombre'
    columnasARecorrer = list(dfApuntadorDePais)
    if not locationNoEstaVacio(document):
        contadorDeAciertos.append("Unknown")
    if locationNoEstaVacio(document):
        for elemento in columnasARecorrer:
            INDEX = 0
            buscadorDeCoincidencias(INDEX, PAIS_NOMBRE, contadorDeAciertos, document, elemento)
    return contadorDeAciertos

fin = len(list(collection.find({},{'_id': 1, 'user.location': 1})))
progreso = 1
inicio = time.time()
for document in collection.find({},{'_id': 1, 'user.location': 1}):
    elPaisMasAcertado = Counter(revisarAcierto(document)).most_common(1)
    userCountry = elPaisMasAcertado[0][0]
    filtro = {'_id': document['_id']}
    nuevoValor = {"$set": {"user.country": userCountry}}
    collection.update_one(filtro, nuevoValor)
    paso = time.time()
    print(str(progreso)+ " / " + str(fin) + " # " + str(progreso/fin) + "%" + " | " + str(paso-inicio))
    progreso+= 1
