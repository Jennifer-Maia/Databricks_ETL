# Databricks notebook source
#Spark
#Insiro as tratativas necessárias e cabiveis na tabela bronze, nesse caso só fizemos um distinct

df = spark.read.json("/Volumes/raw/pokemon/pokemon_raw/pokemons_list/")
df.distinct().display()

# COMMAND ----------

df.distinct().coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze.pokemon.pokemon_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pokemon.pokemon_list
