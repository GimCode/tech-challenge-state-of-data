CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_resumo_pesquisa (
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/resumo_pesquisa/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_perfil_profissionais (
    cargo_atual STRING,
    nivel_carreira STRING,
    regiao STRING,
    modelo_trabalho STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/perfil_profissionais/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_remuneracao (
    faixa_salarial STRING,
    nivel_carreira STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/remuneracao/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_mercado_trabalho (
    oportunidade_buscada STRING,
    regiao STRING,
    nivel_carreira STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/mercado_trabalho/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_tecnologias (
    cloud_dia_a_dia STRING,
    cloud_preferida STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/tecnologias/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_inteligencia_artificial (
    uso_ia_trabalho STRING,
    nivel_carreira STRING,
    regiao STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/inteligencia_artificial/';

CREATE EXTERNAL TABLE IF NOT EXISTS tech_challenge_state_of_data.gold_diversidade (
    genero STRING,
    regiao STRING,
    nivel_carreira STRING,
    quantidade_profissionais BIGINT
)
PARTITIONED BY (periodo_pesquisa STRING)
STORED AS PARQUET
LOCATION 's3://tech-challenge-state-of-data/gold/state_of_data/diversidade/';

MSCK REPAIR TABLE tech_challenge_state_of_data.gold_resumo_pesquisa;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_perfil_profissionais;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_remuneracao;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_mercado_trabalho;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_tecnologias;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_inteligencia_artificial;
MSCK REPAIR TABLE tech_challenge_state_of_data.gold_diversidade;
