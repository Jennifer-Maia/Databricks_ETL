# Databricks notebook source
dbutils.fs.mkdirs("/Volumes/raw/pokemon/pokemon_raw/pokemons_details/")

# COMMAND ----------

#Criando uma função para pegar o dado e salvar no volume para pegar os detalhes, porque só tinha pego os nomes e os números. Agora teremos uma nova ingestão com os detalhes, e depois tudo vai se conversar a frente.
#Isso é útil: Tenho diferentes ingestões (uma é API, outra é excel, outra SQL), ai em cada notebook faço uma ingestão, salvo, crio uma tabela e depois tudo pode se conversar fazendo os joins entre eles e etc
import requests
import datetime
import json
from multiprocessing import Pool

def get_and_save(url):
    data = requests.get(url).json()
    now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    filename = f"/Volumes/raw/pokemon/pokemon_raw/pokemons_details/{data['id']}_{now}.json"
    with open(filename, 'w') as open_file:
        json.dump(data, open_file)

df = spark.table("bronze.pokemon.pokemon_list")
urls = df.select("url").toPandas()["url"].tolist()

#Posso fazer com for ou com multiprocessing
# for u in tqdm(urls):
#     get_and_save(u)

##multiprocessing
with Pool(4) as p:
    print(p.map(get_and_save, urls))


# COMMAND ----------

df = spark.table("bronze.pokemon.pokemon_list")
#df.select("url").display()
urls = df.select("url").toPandas()["url"].tolist()
urls

# COMMAND ----------

import requests

url = 'https://pokeapi.co/api/v2/pokemon/33'
data = requests.get(url).json()
data['id']

# COMMAND ----------

#Visualizar os dados
df = spark.read.json("/Volumes/raw/pokemon/pokemon_raw/pokemons_details/")
df.display()
