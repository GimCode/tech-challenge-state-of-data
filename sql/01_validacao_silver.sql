CREATE EXTERNAL TABLE IF NOT EXISTS silver_state_of_data (
    id_resposta string,
    idade int,
    faixa_idade string,
    genero string,
    uf string,
    regiao string,
    nivel_ensino string,
    cargo_atual string,
    nivel_carreira string,
    faixa_salarial string,
    experiencia_dados string,
    experiencia_ti string,
    modelo_trabalho string,
    cloud_dia_a_dia string,
    cloud_preferida string,
    uso_ia_trabalho string,
    oportunidade_buscada string,
    ano_pesquisa int,
    arquivo_origem string,
    data_ingestao timestamp
)
PARTITIONED BY (periodo_pesquisa string)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/silver/state_of_data/dados_harmonizados/';

MSCK REPAIR TABLE silver_state_of_data;

SELECT periodo_pesquisa, COUNT(*) AS quantidade
FROM silver_state_of_data
GROUP BY periodo_pesquisa
ORDER BY periodo_pesquisa;

SELECT periodo_pesquisa, COUNT(DISTINCT id_resposta) AS respondentes
FROM silver_state_of_data
GROUP BY periodo_pesquisa
ORDER BY periodo_pesquisa;
