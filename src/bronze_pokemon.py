# Databricks notebook source
#Spark
#Insiro as tratativas necessárias e cabiveis na tabela bronze, nesse caso só fizemos um distinct

#Insiro uma parametrização aqui para não precisar criar uma bronze para cada ingestão da raw
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema according to your JSON structure
schema = StructType([
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("level", IntegerType(), True)
])

dbutils.widgets.text(
    "table",
    "",
    "Table Name"
)
table = dbutils.widgets.get("table")

df = spark.read.json(
    f"/Volumes/raw/pokemon/pokemon_raw/{table}/",
    schema=schema
)

(df.distinct()
    .coalesce(1)
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable(f"pokemon_{table}")
)

# COMMAND ----------

##df.distinct().coalesce(1).write.format("delta").mode("overwrite").saveAsTable("bronze.pokemon.pokemon_list")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.pokemon.pokemon_list
