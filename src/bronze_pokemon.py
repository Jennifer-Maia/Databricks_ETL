# Databricks notebook source
#Spark
#Insiro as tratativas necessárias e cabiveis na tabela bronze, nesse caso só fizemos um distinct

#Insiro uma parametrização aqui para não precisar criar uma bronze para cada ingestão da raw
table = dbutils.widgets.get("table")

df = spark.read.json(f"/Volumes/raw/pokemon/pokemon_raw/{table}/")
#df.distinct().display()
(df.distinct()
    .coalesce(1)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"bronze.pokemon.{table}"))

# COMMAND ----------

##df.distinct().coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze.pokemon.pokemon_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pokemon.pokemon_list
