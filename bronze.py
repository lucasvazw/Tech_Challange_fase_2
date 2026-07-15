from pyspark.sql.functions import current_timestamp, lit

anos = [24, 25]

entidades = [
    "aluno",
    "municipio",
    "estado",
    "item",
]


def criar_bronze(entidade, anos, origem="BigQuery"):
    tabela_destino = f"workspace.default.bronze_{entidade}"

    if spark.catalog.tableExists(tabela_destino):
        spark.sql(f"DROP TABLE {tabela_destino}")

    for ano in anos:
        tabela_origem = f"workspace.default.df_{entidade}_{ano}"

        df = (
            spark.table(tabela_origem)
            .withColumn("dt_ingestao", current_timestamp())
            .withColumn("origem", lit(origem))
        )

        (
            df.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(tabela_destino)
        )

        print(f"{tabela_origem} adicionada à {tabela_destino}")


for entidade in entidades:
    criar_bronze(entidade, anos)

print("Todas as tabelas Bronze foram criadas com sucesso!")