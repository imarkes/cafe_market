CREATE OR REPLACE TABLE fact_indicadores AS

WITH selic AS (
    SELECT
        data,
        MAX(valor) AS selic_observada_pct
    FROM read_parquet(
        '{selic}',
        union_by_name = true
    )
    WHERE data IS NOT NULL
    GROUP BY data
),

ipca AS (
    SELECT
        date_trunc('month', data)::DATE AS mes_referencia_ipca,
        MAX(valor) AS ipca_mensal_pct
    FROM read_parquet(
        '{ipca}',
        union_by_name = true
    )
    WHERE data IS NOT NULL
    GROUP BY date_trunc('month', data)::DATE
),

datas AS (
    SELECT data FROM selic

    UNION ALL

    SELECT mes_referencia_ipca
    FROM ipca

    UNION ALL

    SELECT last_day(mes_referencia_ipca)
    FROM ipca
),

limites AS (
    SELECT
        MIN(data) AS min_data,
        MAX(data) AS max_data
    FROM datas
),

calendario AS (
    SELECT
        d.data::DATE AS data
    FROM limites,
    generate_series(
        min_data,
        max_data,
        INTERVAL 1 DAY
    ) AS d(data)
),

base AS (
    SELECT
        c.data,
        s.selic_observada_pct,
        i.ipca_mensal_pct,
        i.mes_referencia_ipca
    FROM calendario c

    LEFT JOIN selic s
        ON c.data = s.data

    LEFT JOIN ipca i
        ON date_trunc('month', c.data)::DATE =
           i.mes_referencia_ipca
)

SELECT
    CAST(strftime(data, '%Y%m%d') AS INTEGER) AS data_key,
    data,

    selic_observada_pct,

    LAST_VALUE(
        selic_observada_pct IGNORE NULLS
    ) OVER (
        ORDER BY data
        ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS selic_ultimo_valor_pct,

    ipca_mensal_pct,
    mes_referencia_ipca,

    selic_observada_pct IS NOT NULL AS flag_selic_observada_dia

FROM base;
