import psycopg2
from matplotlib import pyplot as plt
from pymongo import MongoClient
from wordcloud import WordCloud

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"

#Mongo conexion
client = MongoClient(MONGO_HOST, MONGO_PUERTO)

#db y collection
database = client[MONGO_DB_NOMBRE]
collection = database[MONGO_COLLECTION_NOMBRE]

#elegir 2 paises:
PAIS_1_COD = "ARG"
PAIS_2_COD = "USA"

LAS_20_MAS_USADAS = 20
diccionarioPalabrasPrimerPais = {}
diccionarioPalabrasSegundoPais = {}

#query primer pais
consultaMapReducePrimerPais = collection.aggregate([
    { "$match": {"user.country": {"$eq": PAIS_1_COD}}},
    { "$project" : { "text" : { "$split": ["$text", " "] } } },
    { "$unwind" : "$text" },
    { "$group" : { "_id":  "$text" , "cantidad" : { "$sum" : 1 } } },
    { "$sort" : { "cantidad" : -1 } }
])

#query segundo pais
consultaMapReduceSegundoPais = collection.aggregate([
    { "$match": {"user.country": {"$eq": PAIS_2_COD}}},
    { "$project" : { "text" : { "$split": ["$text", " "] } } },
    { "$unwind" : "$text" },
    { "$group" : { "_id":  "$text" , "cantidad" : { "$sum" : 1 } } },
    { "$sort" : { "cantidad" : -1 } }
])

#palabras a lista
for elemento in consultaMapReducePrimerPais:
    diccionarioPalabrasPrimerPais[elemento["_id"]] = elemento["cantidad"]

for elemento in consultaMapReduceSegundoPais:
    diccionarioPalabrasSegundoPais[elemento["_id"]] = elemento["cantidad"]

#las 20 mas usadas
lasMasUsadasDelPrimerPais = list(diccionarioPalabrasPrimerPais.items())[:LAS_20_MAS_USADAS]
lasMasUsadasDelSegundoPais = list(diccionarioPalabrasSegundoPais.items())[:LAS_20_MAS_USADAS]

#crear y mostrar nubes
nubeDePalabrasPrimerPais = WordCloud(
    background_color='white',
    width=1500,
    height=1000)

nubeDePalabrasSegundoPais = WordCloud(
    background_color='white',
    width=1500,
    height=1000)

#convertir a dict para generar nube
nubeDePalabrasPrimerPais.generate_from_frequencies(dict(lasMasUsadasDelPrimerPais))
nubeDePalabrasSegundoPais.generate_from_frequencies(dict(lasMasUsadasDelSegundoPais))

#imagenes
plt.figure(PAIS_1_COD, figsize=(9, 6))
plt.title(PAIS_1_COD)
plt.imshow(nubeDePalabrasPrimerPais)
plt.axis('off')

plt.figure(PAIS_2_COD, figsize=(9, 6))
plt.title(PAIS_2_COD)
plt.imshow(nubeDePalabrasSegundoPais)
plt.axis('off')

plt.show()