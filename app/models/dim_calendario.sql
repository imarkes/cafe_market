CREATE OR REPLACE TABLE dim_calendario AS

WITH all_dates AS (
    SELECT data FROM fact_preco_cafe
    UNION ALL
    SELECT data FROM fact_clima
    UNION ALL
    SELECT data FROM fact_indicadores
),

bounds AS (
    SELECT
        MIN(data) AS min_data,
        MAX(data) AS max_data
    FROM all_dates
    WHERE data IS NOT NULL
)

SELECT
    CAST(strftime(d.data, '%Y%m%d') AS INTEGER) AS data_key,
    d.data::DATE AS data,

    EXTRACT(year FROM d.data)::INTEGER AS ano,
    EXTRACT(month FROM d.data)::INTEGER AS mes,
    EXTRACT(day FROM d.data)::INTEGER AS dia,

    EXTRACT(quarter FROM d.data)::INTEGER AS trimestre,
    EXTRACT(week FROM d.data)::INTEGER AS semana_ano,

    CASE
        WHEN EXTRACT(month FROM d.data) <= 6 THEN 1
        ELSE 2
    END AS semestre,

    EXTRACT(dow FROM d.data)::INTEGER AS dia_semana_numero,
    strftime(d.data, '%A') AS dia_semana_nome,
    strftime(d.data, '%B') AS nome_mes,
    strftime(d.data, '%Y-%m') AS ano_mes,

    date_trunc('month', d.data)::DATE AS primeiro_dia_mes,
    last_day(d.data)::DATE AS ultimo_dia_mes,

    EXTRACT(dow FROM d.data) IN (0, 6) AS fim_de_semana,
    d.data::DATE = last_day(d.data)::DATE AS ultimo_dia_do_mes

FROM bounds,
generate_series(
    min_data,
    max_data,
    INTERVAL 1 DAY
) AS d(data);
