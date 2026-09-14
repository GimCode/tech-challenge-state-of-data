import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit, trim
from pyspark.sql.types import IntegerType
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "tech-challenge-state-of-data"
BRONZE_PATH = f"s3://{BUCKET}/bronze/state_of_data"
SILVER_PATH = f"s3://{BUCKET}/silver/state_of_data/dados_harmonizados"

df_2022 = spark.read.parquet(f"{BRONZE_PATH}/2022")
df_2024 = spark.read.parquet(f"{BRONZE_PATH}/2024")
df_2025 = spark.read.parquet(f"{BRONZE_PATH}/2025_2026")

def get_column(df, candidates, default=None):
    for candidate in candidates:
        if candidate in df.columns:
            return col(candidate)
    return lit(default)

def selecionar_2022(df):
    return df.select(
        get_column(df, ["('P0', 'id')"]).alias("id_resposta"),
        get_column(df, ["('P1_a ', 'Idade')"]).alias("idade"),
        get_column(df, ["('P1_a_1 ', 'Faixa idade')"]).alias("faixa_idade"),
        get_column(df, ["('P1_b ', 'Genero')"]).alias("genero"),
        get_column(df, ["('P1_i_1 ', 'uf onde mora')"]).alias("uf"),
        get_column(df, ["('P1_i_2 ', 'Regiao onde mora')"]).alias("regiao"),
        get_column(df, ["('P1_l ', 'Nivel de Ensino')"]).alias("nivel_ensino"),
        get_column(df, ["('P2_f ', 'Cargo Atual')"]).alias("cargo_atual"),
        get_column(df, ["('P2_g ', 'Nivel')"]).alias("nivel_carreira"),
        get_column(df, ["('P2_h ', 'Faixa salarial')"]).alias("faixa_salarial"),
        get_column(df, ["('P2_i ', 'Quanto tempo de experiência na área de dados você tem?')"]).alias("experiencia_dados"),
        get_column(df, ["('P2_j ', 'Quanto tempo de experiência na área de TI/Engenharia de Software você teve antes de começar a trabalhar na área de dados?')"]).alias("experiencia_ti"),
        get_column(df, ["('P2_p ', 'Atualmente qual a sua forma de trabalho?')"]).alias("modelo_trabalho"),
        get_column(df, ["('P4_g ', 'Quais das opções de Cloud listadas abaixo você utiliza no trabalho?')"]).alias("cloud_dia_a_dia"),
        get_column(df, ["('P4_h ', 'Dentre as opções listadas, qual sua Cloud preferida?')"]).alias("cloud_preferida"),
        lit(None).alias("uso_ia_trabalho"),
        get_column(df, ["('P5_b ', 'Qual oportunidade você está buscando?')"]).alias("oportunidade_buscada"),
        col("ano_pesquisa"), col("periodo_pesquisa"), col("arquivo_origem"), col("data_ingestao")
    )

def selecionar_2024(df):
    return df.select(
        get_column(df, ["0.a_token"]).alias("id_resposta"),
        get_column(df, ["1.a_idade"]).alias("idade"),
        get_column(df, ["1.a.1_faixa_idade"]).alias("faixa_idade"),
        get_column(df, ["1.b_genero"]).alias("genero"),
        get_column(df, ["1.i.1_uf_onde_mora"]).alias("uf"),
        get_column(df, ["1.i.2_regiao_onde_mora"]).alias("regiao"),
        get_column(df, ["1.l_nivel_de_ensino"]).alias("nivel_ensino"),
        get_column(df, ["2.f_cargo_atual"]).alias("cargo_atual"),
        get_column(df, ["2.g_nivel"]).alias("nivel_carreira"),
        get_column(df, ["2.h_faixa_salarial"]).alias("faixa_salarial"),
        get_column(df, ["2.i_tempo_de_experiencia_em_dados"]).alias("experiencia_dados"),
        get_column(df, ["2.j_tempo_de_experiencia_em_ti"]).alias("experiencia_ti"),
        get_column(df, ["2.r_modelo_de_trabalho_atual"]).alias("modelo_trabalho"),
        get_column(df, ["4.h_cloud_(dia_a_dia)"]).alias("cloud_dia_a_dia"),
        get_column(df, ["4.i_cloud_preferida"]).alias("cloud_preferida"),
        get_column(df, ["4.m_usa_chatgpt_ou_copilot_no_trabalho?"]).alias("uso_ia_trabalho"),
        get_column(df, ["5.b_oportunidade_buscada"]).alias("oportunidade_buscada"),
        col("ano_pesquisa"), col("periodo_pesquisa"), col("arquivo_origem"), col("data_ingestao")
    )

def selecionar_2025(df):
    return df.select(
        get_column(df, ["0.a_token"]).alias("id_resposta"),
        get_column(df, ["1.a_idade"]).alias("idade"),
        get_column(df, ["1.a.1_faixa_idade"]).alias("faixa_idade"),
        get_column(df, ["1.b_genero"]).alias("genero"),
        get_column(df, ["1.i.1_uf_onde_mora"]).alias("uf"),
        get_column(df, ["1.i.2_regiao_onde_mora"]).alias("regiao"),
        get_column(df, ["1.l_nivel_de_ensino"]).alias("nivel_ensino"),
        get_column(df, ["2.f_cargo_atual"]).alias("cargo_atual"),
        get_column(df, ["2.g_nivel"]).alias("nivel_carreira"),
        get_column(df, ["2.h_faixa_salarial"]).alias("faixa_salarial"),
        get_column(df, ["2.i_tempo_de_experiencia_em_dados"]).alias("experiencia_dados"),
        get_column(df, ["2.j_tempo_de_experiencia_em_ti"]).alias("experiencia_ti"),
        get_column(df, ["2.q_modelo_de_trabalho_atual"]).alias("modelo_trabalho"),
        get_column(df, ["4.e_cloud_(dia_a_dia)"]).alias("cloud_dia_a_dia"),
        get_column(df, ["4.f_cloud_preferida"]).alias("cloud_preferida"),
        get_column(df, ["4.j_usa_chatgpt_ou_copilot_no_trabalho?"]).alias("uso_ia_trabalho"),
        get_column(df, ["5.b_oportunidade_buscada"]).alias("oportunidade_buscada"),
        col("ano_pesquisa"), col("periodo_pesquisa"), col("arquivo_origem"), col("data_ingestao")
    )

silver = (
    selecionar_2022(df_2022)
    .unionByName(selecionar_2024(df_2024))
    .unionByName(selecionar_2025(df_2025))
)

string_cols = [
    "id_resposta", "faixa_idade", "genero", "uf", "regiao", "nivel_ensino",
    "cargo_atual", "nivel_carreira", "faixa_salarial", "experiencia_dados",
    "experiencia_ti", "modelo_trabalho", "cloud_dia_a_dia", "cloud_preferida",
    "uso_ia_trabalho", "oportunidade_buscada", "periodo_pesquisa", "arquivo_origem"
]

for field in string_cols:
    silver = silver.withColumn(field, trim(col(field).cast("string")))

silver = (
    silver
    .withColumn("idade", col("idade").cast(IntegerType()))
    .dropDuplicates(["id_resposta", "periodo_pesquisa"])
)

silver.write.mode("overwrite").partitionBy("periodo_pesquisa").parquet(SILVER_PATH)

job.commit()
