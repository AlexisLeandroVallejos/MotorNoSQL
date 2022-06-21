import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from pymongo import MongoClient
import math

#constantes
RUTA_ARCHIVO = r'.\ne_10m_admin_0_countries.shp'
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets_by_country_result"

gpdworld = gpd.read_file(RUTA_ARCHIVO).rename(columns = {'ADM0_A3': 'code'})

#Mongo conexion
try:
    client = MongoClient(MONGO_HOST, MONGO_PUERTO)
    database = client[MONGO_DB_NOMBRE]
    collection = database[MONGO_COLLECTION_NOMBRE]
except:
    print("Could not connect to MongoDB")

cursor = collection.find({'value': {"$ne":"Unknown"}})
df = pd.DataFrame(list(cursor)).rename(columns={'_id': 'code'})
df['value'] = df['value'].apply(lambda x: math.log10(x))

mergedDf = pd.merge(
        gpdworld, df,
        on='code', how='inner'
    )

poblacionMundial = mergedDf.plot(
    column='value',
    cmap = 'Greens',
    alpha=0.5,
    categorical=False,
    legend=True,
    ax=None)


poblacionMundial.set_title("Tweets Mundial")
plt.show()