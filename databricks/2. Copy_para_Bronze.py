# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Inbound para camada Bronze

# COMMAND ----------

from pyspark.sql.functions import col



# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadasdata/bronze/Athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadasdata/bronze/Coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadasdata/bronze/EntriesGender.csv")
medals = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadasdata/bronze/Medals.csv")
teams = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadasdata/bronze/Teams.csv")

# COMMAND ----------

# Escrevendo DataFrames nos Delta tables
athletes.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/athletes")
coaches.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/coaches")
entriesgender.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/entriesgender")
medals.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/medals")
teams.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/teams")
