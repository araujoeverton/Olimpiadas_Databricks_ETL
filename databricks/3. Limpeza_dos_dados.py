# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Manipulando os dados

# COMMAND ----------

athletes = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/bronze/athletes")
coaches = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/bronze/coaches")
entriesgender = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/bronze/entriesgender")
medals = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/bronze/medals")
teams = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/bronze/teams")

# COMMAND ----------

colunas = ["Rank","TeamNOC", "Gold"]
medals.select(colunas)
goldmedals = medals.select(colunas)

# COMMAND ----------

display(goldmedals)

# COMMAND ----------

dispĺay(dbutils.fs.ls("/mnt/olimpiadas-dados/bronze/athletes"))
