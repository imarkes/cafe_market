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
)

SELECT
    d.data::DATE AS data,
    EXTRACT(year FROM d.data)::INTEGER AS ano,
    EXTRACT(month FROM d.data)::INTEGER AS mes,
    EXTRACT(day FROM d.data)::INTEGER AS dia,
    EXTRACT(quarter FROM d.data)::INTEGER AS trimestre,
    strftime(d.data, '%Y-%m') AS ano_mes,
    date_trunc('month', d.data)::DATE AS primeiro_dia_mes,
    last_day(d.data)::DATE AS ultimo_dia_mes
FROM bounds b,
generate_series(
    b.min_data,
    b.max_data,
    INTERVAL 1 DAY
) AS d(data);
