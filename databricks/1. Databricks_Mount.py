# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Configurando a conexão do Azure Databricks

# COMMAND ----------


app_id = "6e37747f-82d6-467e-bc4e-7359074ce0f5"
tenant_id = "82a78a41-ae87-403f-bfaa-570fa0b8e867"
key = "Q4ZFYRNyRd1GjT3MUUabxl5se4iHLh+O3whmlTdl0cruitUX5Gi+nQmqsNdEdZ96l45LzPwCgqpv+AStxzQEQg=="

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": app_id ,
          "fs.azure.account.oauth2.client.secret": key,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

dbutils.fs.mount(
  source = "abfss://data-olympic-data@strolimpiadastokyo.dfs.core.windows.net/",
  mount_point = "/mnt/olimpiadas-dados",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olimpiadas-dados"
