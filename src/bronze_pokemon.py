# Databricks notebook source
#Spark
#Insiro as tratativas necessárias e cabiveis na tabela bronze, nesse caso só fizemos um distinct

#Insiro uma parametrização aqui para não precisar criar uma bronze para cada ingestão da raw
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("level", IntegerType(), True)
])

# Caminho raiz
root_path = "/Volumes/raw/pokemon/pokemon_raw/"

# Lista todas as subpastas dentro de pokemon_raw
folders = [f.name.replace("/", "") for f in dbutils.fs.ls(root_path)]

for folder in folders:
    print(f"Processando pasta: {folder}")
    
    df = spark.read.json(f"{root_path}{folder}/", schema=schema)

    (df.distinct()
       .coalesce(1)
       .write.format("delta")
       .mode("overwrite")
       .saveAsTable(f"pokemon_{folder}"))

print("✅ Finalizado! Todas as tabelas foram criadas.")


# COMMAND ----------

##df.distinct().coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze.pokemon.pokemon_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pokemon.pokemon_list
