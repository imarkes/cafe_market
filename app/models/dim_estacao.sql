CREATE OR REPLACE TABLE dim_estacao AS

WITH estacoes AS (

    SELECT
        codigo_estacao,
        nome,
        'patrocinio' AS cidade,
        'cerrado_mineiro' AS regiao_cafe,
        latitude,
        longitude,
        altitude,
        situacao,
        periodicidade_da_medicao,
        data_inicial,
        data_final
    FROM read_parquet(
        '{inmet_patrocinio}',
        union_by_name = true
    )

    UNION ALL

    SELECT
        codigo_estacao,
        nome,
        'franca' AS cidade,
        'alta_mogiana' AS regiao_cafe,
        latitude,
        longitude,
        altitude,
        situacao,
        periodicidade_da_medicao,
        data_inicial,
        data_final
    FROM read_parquet(
        '{inmet_franca}',
        union_by_name = true
    )
)

SELECT
    codigo_estacao,
    MAX(nome) AS nome,
    MAX(cidade) AS cidade,
    MAX(regiao_cafe) AS regiao_cafe,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude,
    MAX(altitude) AS altitude,
    MAX(situacao) AS situacao,
    MAX(periodicidade_da_medicao) AS periodicidade_da_medicao,
    MIN(data_inicial) AS data_inicial,
    MAX(data_final) AS data_final
FROM estacoes
WHERE codigo_estacao IS NOT NULL
GROUP BY codigo_estacao;
