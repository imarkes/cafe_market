CREATE OR REPLACE TABLE fact_indicadores AS

WITH selic AS (
    SELECT
        data,
        valor AS selic
    FROM read_parquet('{selic}')
),

ipca_mensal AS (
    SELECT
        data AS data_mes,
        valor AS ipca,
        date_trunc('month', data)::DATE AS primeiro_dia_mes,
        last_day(data)::DATE AS ultimo_dia_mes
    FROM read_parquet('{ipca}')
),

ipca_diario AS (
    SELECT
        d.data::DATE AS data,
        i.ipca
    FROM ipca_mensal i,
    generate_series(
        i.primeiro_dia_mes,
        i.ultimo_dia_mes,
        INTERVAL 1 DAY
    ) AS d(data)
)

SELECT
    COALESCE(s.data, i.data) AS data,
    s.selic,
    i.ipca
FROM selic s
FULL JOIN ipca_diario i USING (data);
