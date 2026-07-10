CREATE OR REPLACE TABLE fact_preco_cafe AS

WITH origem AS (

    SELECT
        data,
        'arabica' AS tipo_cafe,
        a_vista_r AS preco_rs_saca,
        a_vista_us AS preco_usd_saca
    FROM read_parquet(
        '{arabica}',
        union_by_name = true
    )

    UNION ALL

    SELECT
        data,
        'robusta' AS tipo_cafe,
        a_vista_r AS preco_rs_saca,
        a_vista_us AS preco_usd_saca
    FROM read_parquet(
        '{robusta}',
        union_by_name = true
    )
),

deduplicado AS (
    SELECT DISTINCT
        data,
        tipo_cafe,
        preco_rs_saca,
        preco_usd_saca
    FROM origem
    WHERE data IS NOT NULL
),

com_lag AS (
    SELECT
        *,
        LAG(preco_rs_saca) OVER (
            PARTITION BY tipo_cafe
            ORDER BY data
        ) AS preco_rs_anterior,

        LAG(preco_usd_saca) OVER (
            PARTITION BY tipo_cafe
            ORDER BY data
        ) AS preco_usd_anterior
    FROM deduplicado
)

SELECT
    CAST(strftime(data, '%Y%m%d') AS INTEGER) AS data_key,
    data,
    tipo_cafe,

    preco_rs_saca,
    preco_usd_saca,

    preco_rs_anterior,
    preco_usd_anterior,

    preco_rs_saca - preco_rs_anterior AS variacao_diaria_rs,

    CASE
        WHEN preco_rs_anterior IS NULL OR preco_rs_anterior = 0
        THEN NULL
        ELSE 100 * (
            preco_rs_saca / preco_rs_anterior - 1
        )
    END AS variacao_diaria_pct,

    CASE
        WHEN preco_rs_saca IS NULL OR preco_rs_saca <= 0
        THEN TRUE
        ELSE FALSE
    END AS flag_preco_invalido

FROM com_lag;
