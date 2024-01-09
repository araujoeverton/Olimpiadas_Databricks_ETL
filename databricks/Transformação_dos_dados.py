# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Transformação dos dados

# COMMAND ----------

path = ("/mnt/olimpiadas-dados/bronze/athletes")
df = spark.read.format("delta").load(path)

# COMMAND ----------

df.show()
