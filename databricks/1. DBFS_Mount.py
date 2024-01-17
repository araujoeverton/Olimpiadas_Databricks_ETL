# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Configurando a conexão do Azure Databricks

# COMMAND ----------

# Montando ABFS (driver de sistema de arquivos de Blob do Azure) 

configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "29e1d04c-4c9d-48f4-99fa-1485e95a9161",
  "fs.azure.account.oauth2.client.secret": "MQv8Q~YV~~4hS3BE~ppDPSqZhHZ8HXDVm3tr4c6m",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/15c23003-2e0f-4294-a1a3-44651083ea70/oauth2/token"
}

dbutils.fs.mount(
  source = "abfss://fsolimpiadas@strolimpiadas.dfs.core.windows.net/",
  mount_point = "/mnt/olimpiadasdata",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('/mnt/olimpiadasdata/bronze')

