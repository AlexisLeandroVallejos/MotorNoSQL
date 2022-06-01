"""
Comandos iniciales en cmd/terminal:

----------Agarra 10000 primeras lineas y las guarda en tweets (5000 tweets)----------
head -n 10000 crisis.20190410.json > tweets.json

----------Mongoimport para crear la bd=test, Collection=tweets, drop si existia, del archivo creado antes----------
mongoimport --db test --collection tweets --drop --file=tweets.json

*Para instalar MongoImport: https://www.mongodb.com/docs/database-tools/installation/installation-windows/#installation
"""

from pymongo import MongoClient
#constantes
HOST = "localhost"
NUMERO_DE_PUERTO = 27017
NOMBRE_DATABASE = "test"
NOMBRE_COLLECTION = "tweets"

#parametros
LOS_PRIMEROS_DIEZ = 10

#agregar conexion al cliente
client = MongoClient(HOST, NUMERO_DE_PUERTO)

database = client[NOMBRE_DATABASE]
collection = database[NOMBRE_COLLECTION]

#1ra Consulta Simple:
consultaUno = collection.find({}, {"_id": 1, "text": 1}).limit(LOS_PRIMEROS_DIEZ)

for elemento in consultaUno:
    print(elemento)