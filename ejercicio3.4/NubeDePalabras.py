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

#constantes query
QUERY_SUM = 1
QUERY_SORT = -1
QUERY_FINDALL_REGEX = r"[A-Za-z0-9áéíóúñ]{2,}"

#listapalabrascomunes
LISTA_PALABRAS_COMUNES_PRIMER_PAIS = ["rt", "no", "que", "de", "la", "las", "en", "el", "co", "es", "los", "con", "del", "al", "lo", "le", "si", "ya", "le", "me", "mi", "te", "yo", "se", "va", "https", "mas", "tan", "una"]
LISTA_PALABRAS_COMUNES_SEGUNDO_PAIS = ["rt", "the", "to", "co", "is", "in", "that", "of", "and", "for", "can", "they", "aoc", "we", "on", "it", "this", "you", "are", "at", "he", "with", "not", "de", "la", "no", "as", "us", "she", "an", "my", "https"]
LAS_20_MAS_USADAS = 20

#elegir 2 paises:
PAIS_1_COD = "ARG"
PAIS_2_COD = "USA"

#diccionarios
diccionarioPalabrasPrimerPais = {}
diccionarioPalabrasSegundoPais = {}

#query primer pais
# Requires the PyMongo package.
# https://api.mongodb.com/python/current
consultaMapReducePrimerPais = collection.aggregate([
    {
        '$match': {
            'user.country': PAIS_1_COD
        }
    }, {
        '$project': {
            'cantidad': {
                '$regexFindAll': {
                    'input': {
                        '$toLower': '$text'
                    },
                    'regex': QUERY_FINDALL_REGEX
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$cantidad'
        }
    }, {
        '$group': {
            '_id': '$cantidad.match',
            'cantidadTotal': {
                '$sum': QUERY_SUM
            }
        }
    }, {
        '$sort': {
            'cantidadTotal': QUERY_SORT
        }
    }
])

#query segundo pais
# Requires the PyMongo package.
# https://api.mongodb.com/python/current
consultaMapReduceSegundoPais = collection.aggregate([
    {
        '$match': {
            'user.country': PAIS_2_COD
        }
    }, {
        '$project': {
            'cantidad': {
                '$regexFindAll': {
                    'input': {
                        '$toLower': '$text'
                    },
                    'regex': QUERY_FINDALL_REGEX
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$cantidad'
        }
    }, {
        '$group': {
            '_id': '$cantidad.match',
            'cantidadTotal': {
                '$sum': QUERY_SUM
            }
        }
    }, {
        '$sort': {
            'cantidadTotal': QUERY_SORT
        }
    }
])

#palabras a diccionarios
for elemento in consultaMapReducePrimerPais:
    diccionarioPalabrasPrimerPais[elemento["_id"]] = elemento["cantidadTotal"]

for elemento in consultaMapReduceSegundoPais:
    diccionarioPalabrasSegundoPais[elemento["_id"]] = elemento["cantidadTotal"]

#filtrar palabras comunes
for clave, valor in list(diccionarioPalabrasPrimerPais.items()):
    if clave in LISTA_PALABRAS_COMUNES_PRIMER_PAIS:
        diccionarioPalabrasPrimerPais.pop(clave, None)

for clave, valor in list(diccionarioPalabrasSegundoPais.items()):
    if clave in LISTA_PALABRAS_COMUNES_SEGUNDO_PAIS:
        diccionarioPalabrasSegundoPais.pop(clave, None)

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