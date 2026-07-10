CREATE OR REPLACE TABLE fact_clima AS

SELECT
    data_medicao AS data,
    'patrocinio' AS cidade,
    'cerrado_mineiro' AS regiao_cafe,
    latitude,
    longitude,
    altitude,
    precipitacao_total_mm,
    temp_max_c,
    temp_media_c,
    temp_min_c,
    umidade_rel_media,
    umidade_rel_minima
FROM read_parquet('{inmet_patrocinio}')

UNION ALL

SELECT
    data_medicao AS data,
    'franca' AS cidade,
    'alta_mogiana' AS regiao_cafe,
    latitude,
    longitude,
    altitude,
    precipitacao_total_mm,
    temp_max_c,
    temp_media_c,
    temp_min_c,
    umidade_rel_media,
    umidade_rel_minima
FROM read_parquet('{inmet_franca}');
