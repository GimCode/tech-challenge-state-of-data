import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import countDistinct
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "tech-challenge-state-of-data"
SILVER_PATH = f"s3://{BUCKET}/silver/state_of_data/dados_harmonizados"
GOLD_PATH = f"s3://{BUCKET}/gold/state_of_data"

silver = spark.read.parquet(SILVER_PATH)

gold_resumo = (
    silver.groupBy("periodo_pesquisa")
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_perfil = (
    silver.groupBy(
        "periodo_pesquisa", "cargo_atual", "nivel_carreira",
        "regiao", "modelo_trabalho"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_remuneracao = (
    silver.groupBy(
        "periodo_pesquisa", "faixa_salarial", "nivel_carreira"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_mercado = (
    silver.groupBy(
        "periodo_pesquisa", "oportunidade_buscada",
        "regiao", "nivel_carreira"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_tecnologias = (
    silver.groupBy(
        "periodo_pesquisa", "cloud_dia_a_dia",
        "cloud_preferida"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_ia = (
    silver.groupBy(
        "periodo_pesquisa", "uso_ia_trabalho",
        "nivel_carreira", "regiao"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

gold_diversidade = (
    silver.groupBy(
        "periodo_pesquisa", "genero",
        "regiao", "nivel_carreira"
    )
    .agg(countDistinct("id_resposta").alias("quantidade_profissionais"))
)

tabelas = {
    "resumo_pesquisa": gold_resumo,
    "perfil_profissionais": gold_perfil,
    "remuneracao": gold_remuneracao,
    "mercado_trabalho": gold_mercado,
    "tecnologias": gold_tecnologias,
    "inteligencia_artificial": gold_ia,
    "diversidade": gold_diversidade
}

for nome, dataframe in tabelas.items():
    dataframe.write.mode("overwrite").partitionBy("periodo_pesquisa").parquet(
        f"{GOLD_PATH}/{nome}"
    )

job.commit()
