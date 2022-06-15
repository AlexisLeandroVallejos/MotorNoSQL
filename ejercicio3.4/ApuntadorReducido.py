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
    queryCountry = "SELECT CODE, NAME FROM COUNTRY"

    #diccionarios para renombrar las columnas
    countryColumnDict = {
        'code': 'codigo',
        'name': 'paisNombre',
    }

    #query
    dfWorldCountry = pd.read_sql(queryCountry, conexion)

    #renombrar
    dfWorldCountry = dfWorldCountry.rename(columns = countryColumnDict)

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

#conocer cuales seran los diferentes criterios
#todosLosLocations = collection.find(
#    {},
#    {"_id": 0, "user.location": 1}).distinct("user.location")
#seriesLocations = Series(todosLosLocations)

#re.escape para evitar caracter espaciales abiertos
def compararStringSimilarDocument_Dataframe(document, elemento):
    return re.search(re.escape(elemento), str(document["user"]["location"]),
                     re.IGNORECASE)

def locationNoEstaVacio(document):
    return document["user"]["location"] != None

def nuevoValor(valor):
    return {"$set": {"user.country": valor}}

fin = len(list(collection.find({},{'_id': 1, 'user.location': 1})))
progreso = 1
inicio = time.time()
DESCONOCIDO = "Unknown"
for document in collection.find({},{'_id': 1, 'user.location': 1}):
    filtro = {'_id': document['_id']}
    PAIS_NOMBRE = 'paisNombre'
    if not locationNoEstaVacio(document):
        collection.update_one(filtro, nuevoValor(DESCONOCIDO))
    if locationNoEstaVacio(document):
        for elemento in dfWorldCountry[PAIS_NOMBRE]:
            if (compararStringSimilarDocument_Dataframe(document, elemento)):
                collection.update_one(filtro, nuevoValor(elemento))
    paso = time.time()
    print(str(progreso)+ " / " + str(fin) + " # " + str((progreso/fin)*100) + "%" + " | " + str(paso-inicio))
    progreso+= 1
