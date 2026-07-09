CREATE OR REPLACE TABLE cafe_market_analysis AS

WITH preco AS (
    SELECT
        data,
        MAX(CASE WHEN tipo_cafe = 'arabica' THEN preco_rs_saca END) AS preco_arabica_rs,
        MAX(CASE WHEN tipo_cafe = 'robusta' THEN preco_rs_saca END) AS preco_robusta_rs
    FROM fact_preco_cafe
    GROUP BY data
),

clima AS (
    SELECT
        data,

        MAX(CASE WHEN cidade = 'patrocinio' THEN precipitacao_total_mm END) AS chuva_patrocinio_mm,
        MAX(CASE WHEN cidade = 'patrocinio' THEN temp_media_c END) AS temp_media_patrocinio_c,
        MAX(CASE WHEN cidade = 'patrocinio' THEN temp_min_c END) AS temp_min_patrocinio_c,
        MAX(CASE WHEN cidade = 'patrocinio' THEN temp_max_c END) AS temp_max_patrocinio_c,
        MAX(CASE WHEN cidade = 'patrocinio' THEN umidade_rel_media END) AS umidade_media_patrocinio,

        MAX(CASE WHEN cidade = 'franca' THEN precipitacao_total_mm END) AS chuva_franca_mm,
        MAX(CASE WHEN cidade = 'franca' THEN temp_media_c END) AS temp_media_franca_c,
        MAX(CASE WHEN cidade = 'franca' THEN temp_min_c END) AS temp_min_franca_c,
        MAX(CASE WHEN cidade = 'franca' THEN temp_max_c END) AS temp_max_franca_c,
        MAX(CASE WHEN cidade = 'franca' THEN umidade_rel_media END) AS umidade_media_franca

    FROM fact_clima
    GROUP BY data
),

base AS (
    SELECT
        c.data,
        c.ano,
        c.mes,
        c.trimestre,
        c.ano_mes,

        p.preco_arabica_rs,
        p.preco_robusta_rs,
        p.preco_arabica_rs - p.preco_robusta_rs AS spread_arabica_robusta_rs,

        i.selic,
        i.ipca,

        cl.chuva_patrocinio_mm,
        cl.temp_media_patrocinio_c,
        cl.temp_min_patrocinio_c,
        cl.temp_max_patrocinio_c,
        cl.umidade_media_patrocinio,

        cl.chuva_franca_mm,
        cl.temp_media_franca_c,
        cl.temp_min_franca_c,
        cl.temp_max_franca_c,
        cl.umidade_media_franca

    FROM dim_calendario c
    LEFT JOIN preco p USING (data)
    LEFT JOIN fact_indicadores i USING (data)
    LEFT JOIN clima cl USING (data)
)

SELECT
    *,

    AVG(preco_arabica_rs) OVER (
        ORDER BY data
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS mm7_preco_arabica_rs,

    AVG(preco_robusta_rs) OVER (
        ORDER BY data
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS mm7_preco_robusta_rs,

    AVG(preco_arabica_rs) OVER (
        ORDER BY data
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS mm30_preco_arabica_rs,

    AVG(preco_robusta_rs) OVER (
        ORDER BY data
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS mm30_preco_robusta_rs,

    SUM(chuva_patrocinio_mm) OVER (
        ORDER BY data
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS chuva_7d_patrocinio_mm,

    SUM(chuva_franca_mm) OVER (
        ORDER BY data
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS chuva_7d_franca_mm,

    SUM(chuva_patrocinio_mm) OVER (
        ORDER BY data
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS chuva_30d_patrocinio_mm,

    SUM(chuva_franca_mm) OVER (
        ORDER BY data
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS chuva_30d_franca_mm

FROM base
ORDER BY data;
