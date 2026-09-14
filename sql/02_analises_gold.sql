SELECT periodo_pesquisa, quantidade_profissionais
FROM gold_resumo_pesquisa
ORDER BY periodo_pesquisa;

SELECT periodo_pesquisa, nivel_carreira,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_perfil_profissionais
GROUP BY periodo_pesquisa, nivel_carreira
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, genero,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_diversidade
GROUP BY periodo_pesquisa, genero
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, uso_ia_trabalho,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_inteligencia_artificial
GROUP BY periodo_pesquisa, uso_ia_trabalho
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, cloud_preferida,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_tecnologias
GROUP BY periodo_pesquisa, cloud_preferida
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, faixa_salarial,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_remuneracao
GROUP BY periodo_pesquisa, faixa_salarial
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, regiao,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_perfil_profissionais
GROUP BY periodo_pesquisa, regiao
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, modelo_trabalho,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_perfil_profissionais
GROUP BY periodo_pesquisa, modelo_trabalho
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;

SELECT periodo_pesquisa, oportunidade_buscada,
       SUM(quantidade_profissionais) AS quantidade_profissionais
FROM gold_mercado_trabalho
GROUP BY periodo_pesquisa, oportunidade_buscada
ORDER BY periodo_pesquisa, quantidade_profissionais DESC;
