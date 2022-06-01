"""
Comandos iniciales en cmd/terminal:

----------Agarra 10000 primeras lineas y las guarda en tweets (5000 tweets)----------
head -n 10000 crisis.20190410.json > tweets.json

----------Mongoimport para crear la bd=test, Collection=tweets, drop si existia, del archivo creado antes----------
mongoimport --db test --collection tweets --drop --file=tweets.json

*Para instalar MongoImport: https://www.mongodb.com/docs/database-tools/installation/installation-windows/#installation
"""


