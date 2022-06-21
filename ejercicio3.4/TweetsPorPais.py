import numpy as np
import psycopg2
import pandas as pd
import geopandas as gpd
from pymongo import MongoClient
from collections import Counter
import matplotlib.pyplot as plt

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
POSTGRESQL_DB_NOMBRE = "world"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_HOST = "localhost"
DESCONOCIDO = "Unknown"

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

    #apuntador para crear diccionario Codigo/Ciudad:
    apuntador = conexion.cursor()

    #queryAPostgresql
    queryCountry = "SELECT CODE, NAME FROM COUNTRY"

    #query
    apuntador.execute(queryCountry)
    listaCodigoNombrePais = apuntador.fetchall()

    #diccionario
    diccionarioCodigoNombrePais = {}

    for (codigo, nombrePais) in listaCodigoNombrePais:
        diccionarioCodigoNombrePais[nombrePais] = codigo

# excepcion por algun error
except (Exception, psycopg2.Error) as error:
    print("Error al obtener la informacion", error)

# cerrar conexion
finally:
    if conexion:
        conexion.close()
        apuntador.close()
        print("Conexion con PostgreSQL cerrada")

#Punto2: geopandas dataframe (mapas de vuelta) y renombrar
gpdworld = gpd.read_file(PATH_WORLD).rename(columns = {'ADM0_A3': 'code'})

#sobre el punto anterior:
#los tweets que tenian el pais, van a tener el nombre de pais en el lugar de su codigo de pais
#este archivo modifica los nombres de pais por codigo de pais en el dataframe para que todos sean iguales en el mapa:

listaDePaises = []

#query solo tweets que tengan user.country y que no sean de paises desconocidos
consulta = collection.find({
    "user.country": {"$exists": "true", "$ne": DESCONOCIDO}},
    {'_id': 0, 'user.country': 1})

#agrega codigos de paises y nombre de paises a lista
for elemento in consulta:
    listaDePaises.append(elemento["user"]["country"])

#si es un nombre de pais lo modifica por codigo de pais
for indice, elemento in enumerate(listaDePaises):
    if elemento in diccionarioCodigoNombrePais:
        listaDePaises[indice] = diccionarioCodigoNombrePais[elemento]

#counter de tweets por pais
contadorDeTweets = Counter(listaDePaises)

#dataframe pais, cantidadtweets
dfCantidadTweets = pd.DataFrame.from_dict(contadorDeTweets, orient='index').reset_index()

#renombre
indicesDataframePais = {'index': 'code', 0: 'cantidadDeTweets'}
dfCantidadTweets = dfCantidadTweets.rename(columns = indicesDataframePais)


#merge, tweets y gpdworld
dataFrameDeTweetsMundial = pd.merge(
    gpdworld, dfCantidadTweets,
    on = 'code', how = 'inner'
)

#aplicar logaritmo para resultado mas claro
dataFrameDeTweetsMundial.cantidadDeTweets = np.log(
    dataFrameDeTweetsMundial.cantidadDeTweets,
    out = np.zeros_like(dataFrameDeTweetsMundial.cantidadDeTweets),
    where = (dataFrameDeTweetsMundial.cantidadDeTweets != 0))

#mapas:
tweetsMundial = dataFrameDeTweetsMundial.plot(
    column='cantidadDeTweets',
    cmap = 'Greens',
    alpha=0.5,
    categorical=False,
    legend=True,
    ax=None)

#titulo
tweetsMundial.set_title("Cantidad de tweets por el mundo")

#mostrar todas
plt.show()