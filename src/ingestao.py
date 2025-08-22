# Databricks notebook source
dbutils.fs.mkdirs("/Volumes/raw/pokemon/pokemon_raw/pokemons_list")

# COMMAND ----------

import requests
import json
import datetime

url = 'https://pokeapi.co/api/v2/pokemon?limit=1500'

resp = requests.get(url)
data = resp.json()


data_save = data['results']
data_save

now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
path = f"/Volumes/raw/pokemon/pokemon_raw/pokemons_list/{now}.json"
with open(path, 'w') as open_file:
  json.dump(data_save, open_file)


# COMMAND ----------

dbutils.fs.ls("/Volumes/raw/pokemon/pokemon_raw/pokemons_list")
