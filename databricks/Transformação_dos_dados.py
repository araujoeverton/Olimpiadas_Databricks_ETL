# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Manipulando os dados

# COMMAND ----------

path = ("/mnt/olimpiadas-dados/bronze/athletes")
df = spark.read.format("delta").load(path)

# COMMAND ----------

df.show()
