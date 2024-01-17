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

colunas_atletas = ["Country","Discipline"]
athletes.select(colunas_atletas)
dados_atletas = athletes.select(colunas_atletas)

# COMMAND ----------

colunas = ["Rank","Team_Country", "Gold"]
medals.select(colunas)
gold_medals = medals.select(colunas)

# COMMAND ----------

colunas = ["Rank","Team_Country", "Silver"]
medals.select(colunas)
silver_medals = medals.select(colunas)

# COMMAND ----------

colunas = ["Rank","Team_Country", "Bronze"]
medals.select(colunas)
bronze_medals = medals.select(colunas)

# COMMAND ----------

# Transferindo DataFrames para camada Silver
dados_atletas.write.format("delta").mode("overwrite").option("overwriteSchema","True").save("/mnt/olimpiadas-dados/silver/dados-atletas")
gold_medals.write.format("delta").mode("overwrite").option("overwriteSchema","True").save("/mnt/olimpiadas-dados/silver/medalhas-ouro")
silver_medals.write.format("delta").mode("overwrite").option("overwriteSchema","True").save("/mnt/olimpiadas-dados/silver/medalhas-prata")
bronze_medals.write.format("delta").mode("overwrite").option("overwriteSchema","True").save("/mnt/olimpiadas-dados/silver/medalhas-bronze")
entriesgender.write.format("delta").mode("overwrite").option("overwriteSchema","True").save("/mnt/olimpiadas-dados/silver/entriesgender")
