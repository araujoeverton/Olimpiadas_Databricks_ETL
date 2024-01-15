# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Configurando a conexão do Azure Databricks

# COMMAND ----------

# Primeiro devemos criar um Scope da key Vault através da Url https://<databricks-instance>#secrets/createScope
# É possível acessar a lista de Scopes atráves do CLI do Databricks a partir do comendo => databricks secrets list-scopes

configs = {
          "fs.azure.account.key.strolimpiadastokyo.blob.core.windows.net": dbutils.secrets.get(scope="key-vault-scope",key="keyolimpiadas")}

# Montagem do DBFS

dbutils.fs.mount(
  source = "abfss://data-olympic-data@strolimpiadastokyo.dfs.core.windows.net/",
  mount_point = "/mnt/olimpiadas-dados",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/olimpiadas-dados"

# COMMAND ----------

dbutils.fs.unmount(
    "/mnt/olimpiadas-dados"
)
