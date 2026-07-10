CREATE OR REPLACE TABLE fact_clima AS

WITH origem AS (

    SELECT
        data_medicao AS data,
        codigo_estacao,
        precipitacao_total_mm,
        temp_max_c,
        temp_media_c,
        temp_min_c,
        umidade_rel_media,
        umidade_rel_minima
    FROM read_parquet(
        '{inmet_patrocinio}',
        union_by_name = true
    )

    UNION ALL

    SELECT
        data_medicao AS data,
        codigo_estacao,
        precipitacao_total_mm,
        temp_max_c,
        temp_media_c,
        temp_min_c,
        umidade_rel_media,
        umidade_rel_minima
    FROM read_parquet(
        '{inmet_franca}',
        union_by_name = true
    )
),

deduplicado AS (
    SELECT DISTINCT
        data,
        codigo_estacao,
        precipitacao_total_mm,
        temp_max_c,
        temp_media_c,
        temp_min_c,
        umidade_rel_media,
        umidade_rel_minima
    FROM origem
    WHERE data IS NOT NULL
)

SELECT
    CAST(strftime(data, '%Y%m%d') AS INTEGER) AS data_key,
    data,
    codigo_estacao,

    precipitacao_total_mm,
    temp_max_c,
    temp_media_c,
    temp_min_c,
    umidade_rel_media,
    umidade_rel_minima,

    temp_max_c - temp_min_c AS amplitude_termica_c,

    CASE
        WHEN precipitacao_total_mm < 0 THEN TRUE
        ELSE FALSE
    END AS flag_precipitacao_invalida,

    CASE
        WHEN temp_min_c > temp_max_c THEN TRUE
        ELSE FALSE
    END AS flag_temperatura_invalida,

    CASE
        WHEN umidade_rel_media NOT BETWEEN 0 AND 100
          OR umidade_rel_minima NOT BETWEEN 0 AND 100
        THEN TRUE
        ELSE FALSE
    END AS flag_umidade_invalida,

    CASE
        WHEN precipitacao_total_mm IS NULL
          OR temp_max_c IS NULL
          OR temp_media_c IS NULL
          OR temp_min_c IS NULL
          OR umidade_rel_media IS NULL
        THEN TRUE
        ELSE FALSE
    END AS flag_registro_incompleto

FROM deduplicado;
