"""
Comandos iniciales en cmd/terminal:

----------Mongoimport----------
mongoimport --db test --collection tweets --drop --file=crisis.20190410.json

----------world----------
todos los archivos del zip de world, trabajopractico3
"""
import re
import psycopg2
import pandas as pd
import geopandas as gpd
from pymongo import MongoClient
from pandas import Series, DataFrame
from functools import reduce

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
POSTGRESQL_DB_NOMBRE = "world"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_HOST = "localhost"

PATH_WORLD = r'.\world\ne_10m_admin_0_countries.shp'

#Mongo conexion
client = MongoClient(MONGO_HOST, MONGO_PUERTO)

database = client[MONGO_DB_NOMBRE]
collection = database[MONGO_COLLECTION_NOMBRE]

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
dfWorldExtras = gpdworld[['ADM0_A3', 'ABBREV', 'POSTAL', 'FORMAL_EN']]

#diccionario para renombrar las columnas
extrasColumnDict = {
    'ADM0_A3': 'codigo',
    'ABBREV': 'abreviatura',
    'POSTAL': 'codigoPostal',
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
todosLosLocations = collection.find(
    {},
    {"_id": 0, "user.location": 1}).distinct("user.location")
seriesLocations = Series(todosLosLocations)

#etc
#for document in collection.find({},{"_id": 0, "user.location": 1}):
#    for index, row in dfApuntadorDePais.iterrows():
#        if (re.match(document["user"]["location"], row["codigo"]))
listaDeAciertos = []

#for document in collection.find({},{"_id": 0, "user.location": 1}):
#    dfResultado = dfApuntadorDePais.apply(
#        lambda row:
#        row.astype(str).str.contains(document["user"]["location"], case=False).any(),
#        axis=1)

for document in collection.find({},{"_id": 1, "user.location": 1}):
    for index, row in dfApuntadorDePais.iterrows():
        if (re.search(document["user"]["location"], row[index], re.IGNORECASE)):
            collection.update_one(
                {"_id": "document._id"},
                {"$set": {"document.user.country": row[1]}},
                "false", "true"
            )
    collection.update_one(
            {"_id": "document._id"},
            {"$set": {"document.user.country": "unknown"}},
            "false", "true"
        )
#filtrar incremental?
#for item in seriesLocations:

