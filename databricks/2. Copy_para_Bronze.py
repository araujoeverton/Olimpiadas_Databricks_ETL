# Databricks notebook source
# MAGIC %md
# MAGIC <h1> Inbound para camada Bronze

# COMMAND ----------

from pyspark.sql.functions import col
from functools import reduce
import re



# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/medals.csv")
teams = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/teams.csv")

# COMMAND ----------

# Substituindo caracteres especiais do header dos DataFrames
invalid_chars = r'[ ,;{}()\n\t=]'
athletes = athletes.toDF(*[re.sub(invalid_chars, '_', c) for c in athletes.columns])
coaches = coaches.toDF(*[re.sub(invalid_chars, '_', c) for c in coaches.columns])
entriesgender = entriesgender.toDF(*[re.sub(invalid_chars, '_', c) for c in entriesgender.columns])
medals = medals.withColumnRenamed("Team/NOC", "TeamNOC")
medals = medals.toDF(*[re.sub(invalid_chars, '_', c) for c in medals.columns])

# Escrevendo DataFrames nos Delta tables
athletes.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/athletes")
coaches.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/coaches")
entriesgender.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/entriesgender")
medals.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/medals")
teams.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/teams")
