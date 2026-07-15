from pyspark.sql.functions import col, count, when, trim
from pyspark.sql.types import StringType


def bronze_to_silver(bronze_table, silver_table, tipo=None):

    print("=" * 70)
    print(f"Processando: {bronze_table}")
    print("=" * 70)

    df = spark.table(bronze_table)

    print("Quantidade de registros na Bronze:", df.count())

    df = df.toDF(*[c.lower() for c in df.columns])

    df = df.dropDuplicates()

    print("Após remover duplicados:", df.count())

    if tipo == "aluno":

        df = df.filter(
            col("in_presenca_lp") == 1
        )

        print("Após filtrar presença:", df.count())

        df = df.filter(
            col("vl_proficiencia_lp").isNotNull()
        )

        print("Após remover notas nulas:", df.count())

    for campo in df.schema.fields:

        if isinstance(campo.dataType, StringType):

            df = df.withColumn(
                campo.name,
                trim(col(campo.name))
            )

    print("Campos texto padronizados!")

    print("Quantidade de valores nulos:")

    display(
        df.select([
            count(
                when(col(c).isNull(), c)
            ).alias(c)
            for c in df.columns
        ])
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    print(f"Tabela Silver criada com sucesso: {silver_table}")

    display(df.limit(20))


bronze_to_silver(
    "workspace.default.bronze_aluno",
    "workspace.default.silver_aluno",
    tipo="aluno"
)

bronze_to_silver(
    "workspace.default.bronze_estado",
    "workspace.default.silver_estado"
)

bronze_to_silver(
    "workspace.default.bronze_municipio",
    "workspace.default.silver_municipio"
)

bronze_to_silver(
    "workspace.default.bronze_item",
    "workspace.default.silver_item"
)