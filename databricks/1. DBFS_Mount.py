# Databricks notebook source
# MAGIC %md
# MAGIC <h1>Configurando a conexão do Azure Databricks

# COMMAND ----------

# Primeiro devemos criar um Scope da key Vault através da Url https://<databricks-instance>#secrets/createScope
# É possível acessar a lista de Scopes atráves do CLI do Databricks a partir do comendo => databricks secrets list-scopes


# Montagem do DBFS

dbutils.fs.mount(
  source = "wasbs://fsolimpiadas@strolimpiadas.blob.core.windows.net",
  mount_point = "/mnt/dev-files",
  extra_configs = {
          "fs.azure.account.key.strolimpiadas.blob.core.windows.net":dbutils.secrets.get(scope="scopeolimpiadas", key="secret-olimpiadas")})

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/dev-files"

# COMMAND ----------

dbutils.fs.unmount(
    "/mnt/olimpiadas-dados"
)
