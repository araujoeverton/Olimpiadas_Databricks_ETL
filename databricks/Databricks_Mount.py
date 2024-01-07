# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Configurando a conexão do Azure Databricks

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "6e37747f-82d6-467e-bc4e-7359074ce0f5",
          "fs.azure.account.oauth2.client.secret":"VTr8Q~8m7JbZkxYc0hgQpjJBgS8ArlBvKP1Nqa6Q",
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/82a78a41-ae87-403f-bfaa-570fa0b8e867/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://data-olympic-data@strolimpiadastokyo.dfs.core.windows.net/",
  mount_point = "/mnt/olimpiadas-dados",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olimpiadas-dados"
