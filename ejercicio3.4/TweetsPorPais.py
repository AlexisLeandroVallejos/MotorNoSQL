import math
import pandas as pd
import geopandas as gpd
from pymongo import MongoClient
import matplotlib.pyplot as plt

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
UNKNOWN = "unknown"

PATH_WORLD = r'.\world\ne_10m_admin_0_countries.shp'

#Mongo conexion
client = MongoClient(MONGO_HOST, MONGO_PUERTO)

#db y collection
database = client[MONGO_DB_NOMBRE]
collection = database[MONGO_COLLECTION_NOMBRE]

#Punto2: geopandas dataframe (mapas de vuelta) y renombrar
gpdworld = gpd.read_file(PATH_WORLD).rename(columns = {'ADM0_A3': 'code'})

#query solo tweets que tengan el campo user.country y no sean desconocidos
consulta = collection.aggregate([
    {
        "$match":
            {
                "user.country": {"$exists": "true", "$ne": UNKNOWN}
            }
    },
    {
        "$group":
            {
                "_id": "$user.country",
                "cantidadDeTweets": { "$sum":  1  }
            }
    }
])

#pasar a diccionario
contadorDeTweets = {}
for elemento in consulta:
    contadorDeTweets[elemento["_id"]] = elemento["cantidadDeTweets"]

#aplicar logaritmo:
diccionarioContadorLog = {codigo : math.log10(cantidadDeTweets) for codigo, cantidadDeTweets in contadorDeTweets.items()}

#dataframe pais, cantidadtweets
dfCantidadTweets = pd.DataFrame.from_dict(diccionarioContadorLog, orient='index').reset_index()

#renombre
indicesDataframePais = {'index': 'code', 0: 'cantidadDeTweets'}
dfCantidadTweets = dfCantidadTweets.rename(columns = indicesDataframePais)

#merge, tweets y gpdworld
dataFrameDeTweetsMundial = pd.merge(
    gpdworld, dfCantidadTweets,
    on = 'code', how = 'inner'
)

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