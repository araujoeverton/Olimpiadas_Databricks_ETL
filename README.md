# Olimpiadas_Tokyo_2021_ETL
## _por Everton Araujo_


Neste projeto, utilizando Apache Spark e o Python para extração e transformação dos dados. Todo o projeto seguiu a convenção de boas práticas do Databricks, com documentação e de fácil aplicabilidade.

## Etapas
- Extração, tratamento e carga de dados com Pyspark.
- Carga em Banco de Dados Azure Data Lake.
- Compartilhamento público do script no GitHub para versionamento e fins de estudo.
- Boas práticas para garantir a segurança dos dados no Blob Storage.





And of course Dillinger itself is open source with a [public repository][dill]
 on GitHub.

## Recursos

Para este projeto foram utilizados:

| Recurso | Links |
| ------ | ------ |
| Grupo de Recursos | [Saiba Mais](https://learn.microsoft.com/pt-br/azure/azure-resource-manager/management/overview)|
| Azure Databricks | [ Documentação](https://learn.microsoft.com/en-us/azure/databricks/) |
| Data Lake Storage Gen2 | [Saber Mais](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)|
|APP Registration | [Saber Mais](https://learn.microsoft.com/pt-br/entra/identity-platform/quickstart-register-app) |
| Azure Data Factory | [Documentação](https://learn.microsoft.com/en-us/azure/data-factory/) |


## Ingestão de dados
Os dados foram retirados de um dataset do Kaggle, que pode ser encontrado no seguinte [link](https://www.kaggle.com/discussions/general/254945)
Toda a ingestão de dados foi feita em blob storage nomeado de  Bronze.

## Montagem do Databricks
A conexão com a Azure, foi realizada com linguagem python, em um notebook já com um cluster provisionado, código [disponível na documentação do Databricks](https://docs.databricks.com/en/dbfs/mounts.html)

```sh
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "<application-id>",
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="<scope-name>",key="<service-credential-key-name>"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/<mount-name>",
  extra_configs = configs)
```

## Tratamento dos dados com Spark
Foi utilizado o InferSchema para atribuir a tipagem correta de forma automatizada nas tabelas.

```ssh
athletes = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/medals.csv")
teams = spark.read.format("csv").option("header","true").option("InferSchema","true").load("/mnt/olimpiadas-dados/bronze/teams.csv")
```
## Remoção de caracteres especiais
```ssh
invalid_chars = r'[ ,;{}()\n\t=]'
athletes = athletes.toDF(*[re.sub(invalid_chars, '_', c) for c in athletes.columns])
coaches = coaches.toDF(*[re.sub(invalid_chars, '_', c) for c in coaches.columns])
entriesgender = entriesgender.toDF(*[re.sub(invalid_chars, '_', c) for c in entriesgender.columns])
medals = medals.withColumnRenamed("Team/NOC", "TeamNOC")
medals = medals.toDF(*[re.sub(invalid_chars, '_', c) for c in medals.columns])
```
## Escrevendo DataFrames nos Delta tables
```ssh
athletes.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/athletes")
coaches.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/coaches")
entriesgender.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/entriesgender")
medals.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/medals")
teams.write.format("delta").mode("overwrite").save("/mnt/olimpiadas-dados/bronze/teams")
```
## Transformando os dados
```ssh

```


