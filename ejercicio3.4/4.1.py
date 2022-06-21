"""
Comandos iniciales en cmd/terminal:

----------Mongoimport----------
mongoimport --db test --collection tweets --drop --file=crisis.20190410.json

----------world----------
todos los archivos del zip de world, trabajopractico3
"""
import sys
import time
import re
import psycopg2
import unidecode
from pymongo import MongoClient

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
POSTGRESQL_DB_NOMBRE = "world"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "123456"
POSTGRESQL_HOST = "localhost"
UNKNOWN = "unknown"
NONE = "none"
CONSTANTE_PORCENTAJE = 100

PATH_WORLD = r'.\world\ne_10m_admin_0_countries.shp'


def normalizedString(string):
    if string != ' ':
        if string[0] == ' ':
            string = string[1:]
        if string[-1] == ' ':
            string = string[:-1]
    return unidecode.unidecode(string.lower())


#Mongo conexion
try:
    client = MongoClient(MONGO_HOST, MONGO_PUERTO)
    database = client[MONGO_DB_NOMBRE]
    collection = database[MONGO_COLLECTION_NOMBRE]
except:
    print("Could not connect to MongoDB")


#postgresql conexion
try:
    conexion = psycopg2.connect(
            database = POSTGRESQL_DB_NOMBRE,
            user = POSTGRESQL_USER,
            password = POSTGRESQL_PASSWORD,
            host = POSTGRESQL_HOST)

    apuntador = conexion.cursor()

    queryCountries = """
        SELECT 
            DISTINCT NAME, 
            CODE 
        FROM 
            COUNTRY
        ORDER BY
            NAME,
            CODE;"""

    queryCities = """
    SELECT 
        DISTINCT NAME, 
        COUNTRYCODE 
    FROM 
        CITY
    ORDER BY
        NAME,
        COUNTRYCODE;"""

    queryDistrict = """
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

    apuntador.execute(queryCountries)
    countryList = apuntador.fetchall()

    apuntador.execute(queryCities)
    cityList = apuntador.fetchall()

    apuntador.execute(queryDistrict)
    districtList = apuntador.fetchall()

    locationDict = {}

    for (country, code) in countryList:
        locationDict[normalizedString(country)] = code

    for (city, code) in cityList:
        locationDict[normalizedString(city)] = code

    for (district, code) in districtList:
        locationDict[normalizedString(district)] = code


# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    print("Error al obtener la informacion", error)

# cerrar conexion
finally:
    if conexion:
        conexion.close()
        apuntador.close()
        print("Conexion con PostgreSQL cerrada")


def getCountryCode(locationString):
    normalizedLocation = normalizedString(locationString)

    for s in re.split(r', |,| - |- |-|\.|\. |/|\\', normalizedLocation):
        if s in locationDict:
            return locationDict[s]

    for s in re.split(r' ', normalizedLocation):
        if s in locationDict:
            return locationDict[s]

    errorList.append(normalizedLocation)
    return UNKNOWN


processedTweets = 0
successCount = 0
totalTweets = len(list(collection.find({'user.location': {"$ne":None}}, {'_id': 1, 'user.location': 1})))
errorList = []

for tweet in collection.find({'user.location': {"$ne":None}}, {'_id': 1, 'user.location': 1}):

    locationString = tweet["user"]["location"]
    countryCode = getCountryCode(locationString)
    #collection.update_one({'_id': tweet["_id"]}, {"$set": {'user.country': countryCode}})

    if countryCode != UNKNOWN:
        successCount += 1

    processedTweets += 1

    sys.stdout.write("\rTweets procesados: {}/{} ({:.2f}%) | Exitosos: {} ({:.2f}%)".format(
        processedTweets,
        totalTweets,
        processedTweets / totalTweets * 100,
        successCount,
        successCount / totalTweets * 100))


print("\nFin del proceso")

