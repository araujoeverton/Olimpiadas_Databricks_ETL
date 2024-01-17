# Databricks notebook source
dados_atletas = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/silver/dados-atletas")
entriesgender = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/silver/entriesgender")
gold_medals = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/silver/medalhas-ouro")
silver_medals = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/silver/medalhas-prata")
bronze_medals = spark.read.format("delta").option("header","true").load("/mnt/olimpiadas-dados/silver/medalhas-bronze")

# COMMAND ----------




# COMMAND ----------


