from pyspark.sql.functions import *
from pyspark.sql.window import Window

aluno = (
    spark.table("workspace.default.silver_aluno")
    .select(
        "id_aluno",           
        "id_escola",          
        "nu_ano_avaliacao",
        "co_uf",
        "co_municipio",
        "tp_dependencia",
        "vl_proficiencia_lp",
        "in_alfabetizado",
        "in_presenca_lp"
    )
)

estado = (
    spark.table("workspace.default.silver_estado")
    .select(
        "co_uf",
        "sg_uf"
    )
    .dropDuplicates(["co_uf"])
)

municipio = (
    spark.table("workspace.default.silver_municipio")
    .select(
        "co_municipio",
        "no_municipio"
    )
    .dropDuplicates(["co_municipio"])
)

print(f"Total de registros aluno: {aluno.count()}")

silver = (
    aluno
    .join(
        municipio,
        on="co_municipio",
        how="left"
    )
    .join(
        estado,
        on="co_uf",
        how="left"
    )
    .withColumn("co_municipio", coalesce(col("co_municipio"), lit(0)))
    .withColumn("co_uf", coalesce(col("co_uf"), lit(0)))
    .withColumn("tp_dependencia", coalesce(col("tp_dependencia"), lit(0)))
    .withColumn(
        "no_municipio",
        when(col("no_municipio").isNull() | (trim(col("no_municipio")) == ""), "Não Informado")
        .otherwise(col("no_municipio"))
    )
    .withColumn(
        "sg_uf",
        when(col("sg_uf").isNull() | (trim(col("sg_uf")) == ""), "Não Informado")
        .otherwise(col("sg_uf"))
    )
)

print(f"Registros após enriquecimento: {silver.count()}")

gold_aluno = (
    silver
    .select(
        "id_aluno",
        "id_escola",
        "nu_ano_avaliacao",
        "co_uf",
        "sg_uf",
        "co_municipio",
        "no_municipio",
        "tp_dependencia",
        "vl_proficiencia_lp",
        "in_alfabetizado",
        "in_presenca_lp"
    )
    .withColumn(
        "status_alfabetizacao",
        when(col("in_alfabetizado") == 1, "Alfabetizado")
        .otherwise("Não Alfabetizado")
    )
    .withColumn(
        "status_presenca_lp",
        when(col("in_presenca_lp") == 1, "Presente")
        .when(col("in_presenca_lp") == 0, "Ausente")
        .otherwise("Não Informado")
    )
    .withColumn(
        "tp_dependencia_desc",
        when(col("tp_dependencia") == 1, "1 - Federal")
        .when(col("tp_dependencia") == 2, "2 - Estadual")
        .when(col("tp_dependencia") == 3, "3 - Municipal")
        .when(col("tp_dependencia") == 4, "4 - Privada")
        .otherwise("Não Informado")
    )
)

(
    gold_aluno.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("nu_ano_avaliacao") 
    .saveAsTable("workspace.default.gold_aluno")
)

print("Gold Aluno criada com sucesso!")

df_municipio_base = (
    silver
    .groupBy(
        "nu_ano_avaliacao",
        "co_uf",
        "sg_uf",
        "co_municipio",
        "no_municipio",
        "tp_dependencia"
    )
    .agg(
        count("*").alias("total_alunos"),
        round(avg("vl_proficiencia_lp"), 2).alias("media_proficiencia_lp"),
        round(avg("in_alfabetizado") * 100, 2).alias("percentual_alfabetizados"),
        round(avg(when(col("in_alfabetizado") == 0, 1).otherwise(0)) * 100, 2).alias("percentual_nao_alfabetizados")
    )
)

window_municipio = Window.partitionBy("co_uf", "co_municipio", "tp_dependencia").orderBy("nu_ano_avaliacao")

gold_indicador_municipio = (
    df_municipio_base
    .withColumn("total_alunos_ano_anterior", lag("total_alunos").over(window_municipio))
    .withColumn("media_proficiencia_lp_ano_anterior", lag("media_proficiencia_lp").over(window_municipio))
    .withColumn("percentual_alfabetizados_ano_anterior", lag("percentual_alfabetizados").over(window_municipio))
    .withColumn("percentual_nao_alfabetizados_ano_anterior", lag("percentual_nao_alfabetizados").over(window_municipio))
    .withColumn(
        "dif_media_proficiencia_lp_yoy", 
        round(col("media_proficiencia_lp") - col("media_proficiencia_lp_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_alfabetizados_pp_yoy", 
        round(col("percentual_alfabetizados") - col("percentual_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_nao_alfabetizados_pp_yoy", 
        round(col("percentual_nao_alfabetizados") - col("percentual_nao_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "var_percentual_total_alunos_yoy",
        round(((col("total_alunos") - col("total_alunos_ano_anterior")) / col("total_alunos_ano_anterior")) * 100, 2)
    )
)

(
    gold_indicador_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("nu_ano_avaliacao") 
    .saveAsTable("workspace.default.gold_indicador_municipio")
)

print("Gold Município criada com sucesso!")

df_estado_base = (
    silver
    .groupBy(
        "nu_ano_avaliacao",
        "co_uf",
        "sg_uf"
    )
    .agg(
        count("*").alias("total_alunos"),
        round(avg("vl_proficiencia_lp"), 2).alias("media_proficiencia_lp"),
        round(avg("in_alfabetizado") * 100, 2).alias("percentual_alfabetizados"),
        round(avg(when(col("in_alfabetizado") == 0, 1).otherwise(0)) * 100, 2).alias("percentual_nao_alfabetizados")
    )
)

window_estado = Window.partitionBy("co_uf").orderBy("nu_ano_avaliacao")

gold_indicador_estado = (
    df_estado_base
    .withColumn("total_alunos_ano_anterior", lag("total_alunos").over(window_estado))
    .withColumn("media_proficiencia_lp_ano_anterior", lag("media_proficiencia_lp").over(window_estado))
    .withColumn("percentual_alfabetizados_ano_anterior", lag("percentual_alfabetizados").over(window_estado))
    .withColumn("percentual_nao_alfabetizados_ano_anterior", lag("percentual_nao_alfabetizados").over(window_estado))
    .withColumn(
        "dif_media_proficiencia_lp_yoy", 
        round(col("media_proficiencia_lp") - col("media_proficiencia_lp_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_alfabetizados_pp_yoy", 
        round(col("percentual_alfabetizados") - col("percentual_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_nao_alfabetizados_pp_yoy", 
        round(col("percentual_nao_alfabetizados") - col("percentual_nao_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "var_percentual_total_alunos_yoy",
        round(((col("total_alunos") - col("total_alunos_ano_anterior")) / col("total_alunos_ano_anterior")) * 100, 2)
    )
)

(
    gold_indicador_estado.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("nu_ano_avaliacao")
    .saveAsTable("workspace.default.gold_indicador_estado")
)

print("Gold Estado criada com sucesso!")

df_brasil_base = (
    silver
    .groupBy(
        "nu_ano_avaliacao"
    )
    .agg(
        count("*").alias("total_alunos"),
        round(avg("vl_proficiencia_lp"), 2).alias("media_proficiencia_lp"),
        round(avg("in_alfabetizado") * 100, 2).alias("percentual_alfabetizados"),
        round(avg(when(col("in_alfabetizado") == 0, 1).otherwise(0)) * 100, 2).alias("percentual_nao_alfabetizados")
    )
)

window_brasil = Window.partitionBy(lit(1)).orderBy("nu_ano_avaliacao")

gold_indicador_brasil = (
    df_brasil_base
    .withColumn("total_alunos_ano_anterior", lag("total_alunos").over(window_brasil))
    .withColumn("media_proficiencia_lp_ano_anterior", lag("media_proficiencia_lp").over(window_brasil))
    .withColumn("percentual_alfabetizados_ano_anterior", lag("percentual_alfabetizados").over(window_brasil))
    .withColumn("percentual_nao_alfabetizados_ano_anterior", lag("percentual_nao_alfabetizados").over(window_brasil))
    .withColumn(
        "dif_media_proficiencia_lp_yoy", 
        round(col("media_proficiencia_lp") - col("media_proficiencia_lp_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_alfabetizados_pp_yoy", 
        round(col("percentual_alfabetizados") - col("percentual_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "dif_percentual_nao_alfabetizados_pp_yoy", 
        round(col("percentual_nao_alfabetizados") - col("percentual_nao_alfabetizados_ano_anterior"), 2)
    )
    .withColumn(
        "var_percentual_total_alunos_yoy",
        round(((col("total_alunos") - col("total_alunos_ano_anterior")) / col("total_alunos_ano_anterior")) * 100, 2)
    )
)

(
    gold_indicador_brasil.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("nu_ano_avaliacao") 
    .saveAsTable("workspace.default.gold_indicador_brasil")
)

print("Gold Brasil criada com sucesso!")

display(gold_aluno.limit(10))
display(gold_indicador_municipio.limit(10))
display(gold_indicador_estado.limit(10))
display(gold_indicador_brasil.limit(10))