import psycopg2
from matplotlib import pyplot as plt
from pymongo import MongoClient
from collections import Counter
from wordcloud import WordCloud

#constantes
MONGO_HOST = "localhost"
MONGO_PUERTO = 27017
MONGO_DB_NOMBRE = "test"
MONGO_COLLECTION_NOMBRE = "tweets"
POSTGRESQL_DB_NOMBRE = "world"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_HOST = "localhost"

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

#elegir 2 paises:
PAIS_1_NAME = "Argentina"
PAIS_2_NAME = "United States"
PAIS_1_COD = diccionarioCodigoNombrePais[PAIS_1_NAME]
PAIS_2_COD = diccionarioCodigoNombrePais[PAIS_2_NAME]
LAS_20_MAS_USADAS = 20

listaDePalabrasPrimerPais = []
listaDePalabrasSegundoPais = []

#query primer pais
consultaPrimerPais = collection.find({
    "$or": [{"user.country": {"$eq": PAIS_1_NAME}}, {"user.country": {"$eq": PAIS_1_COD}}]},
     {'_id': 0, 'text': 1})

#query segundo pais
consultaSegundoPais = collection.find({
    "$or": [{"user.country": {"$eq": PAIS_2_NAME}}, {"user.country": {"$eq": PAIS_2_COD}}]},
     {'_id': 0, 'text': 1})


#palabras a lista
for tweet in consultaPrimerPais:
    linea = tweet["text"].strip()
    palabras = linea.split()
    for palabra in palabras:
        listaDePalabrasPrimerPais.append(palabra)

for tweet in consultaSegundoPais:
    linea = tweet["text"].strip()
    palabras = linea.split()
    for palabra in palabras:
        listaDePalabrasSegundoPais.append(palabra)

#contar palabras
contadorDePalabrasPrimerPais = Counter(listaDePalabrasPrimerPais)
contadorDePalabrasSegundoPais = Counter(listaDePalabrasSegundoPais)

#las 20 mas usadas
lasMasUsadasDelPrimerPais = contadorDePalabrasPrimerPais.most_common(LAS_20_MAS_USADAS)
lasMasUsadasDelSegundoPais = contadorDePalabrasSegundoPais.most_common(LAS_20_MAS_USADAS)

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
plt.figure(PAIS_1_NAME, figsize=(9, 6))
plt.title(PAIS_1_NAME)
plt.imshow(nubeDePalabrasPrimerPais)
plt.axis('off')

plt.figure(PAIS_2_NAME, figsize=(9, 6))
plt.title(PAIS_2_NAME)
plt.imshow(nubeDePalabrasSegundoPais)
plt.axis('off')

plt.show()