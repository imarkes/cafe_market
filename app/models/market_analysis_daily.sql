CREATE OR REPLACE TABLE cafe_market_analysis_daily AS

WITH preco_metricas AS (
    SELECT
        data,
        tipo_cafe,
        preco_rs_saca,
        preco_usd_saca,
        variacao_diaria_pct,

        AVG(preco_rs_saca) OVER (
            PARTITION BY tipo_cafe
            ORDER BY data
            RANGE BETWEEN INTERVAL 6 DAYS PRECEDING
            AND CURRENT ROW
        ) AS media_preco_7d,

        AVG(preco_rs_saca) OVER (
            PARTITION BY tipo_cafe
            ORDER BY data
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING
            AND CURRENT ROW
        ) AS media_preco_30d

    FROM fact_preco_cafe
),

preco AS (
    SELECT
        data,

        MAX(preco_rs_saca)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS preco_arabica_rs,

        MAX(preco_rs_saca)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS preco_robusta_rs,

        MAX(variacao_diaria_pct)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS variacao_arabica_pct,

        MAX(variacao_diaria_pct)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS variacao_robusta_pct,

        MAX(media_preco_7d)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS media_7d_arabica_rs,

        MAX(media_preco_7d)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS media_7d_robusta_rs,

        MAX(media_preco_30d)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS media_30d_arabica_rs,

        MAX(media_preco_30d)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS media_30d_robusta_rs

    FROM preco_metricas
    GROUP BY data
),

clima_base AS (
    SELECT
        f.*,
        e.cidade,
        e.regiao_cafe,

        SUM(precipitacao_total_mm) OVER (
            PARTITION BY f.codigo_estacao
            ORDER BY f.data
            RANGE BETWEEN INTERVAL 6 DAYS PRECEDING
            AND CURRENT ROW
        ) AS chuva_acumulada_7d_mm,

        SUM(precipitacao_total_mm) OVER (
            PARTITION BY f.codigo_estacao
            ORDER BY f.data
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING
            AND CURRENT ROW
        ) AS chuva_acumulada_30d_mm,

        AVG(temp_media_c) OVER (
            PARTITION BY f.codigo_estacao
            ORDER BY f.data
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING
            AND CURRENT ROW
        ) AS temp_media_movel_30d_c,

        STDDEV_SAMP(temp_media_c) OVER (
            PARTITION BY f.codigo_estacao
            ORDER BY f.data
            RANGE BETWEEN INTERVAL 29 DAYS PRECEDING
            AND CURRENT ROW
        ) AS temp_desvio_padrao_30d_c

    FROM fact_clima f

    INNER JOIN dim_estacao e
        ON f.codigo_estacao = e.codigo_estacao
),

clima_metricas AS (
    SELECT
        *,

        temp_media_c - temp_media_movel_30d_c
            AS anomalia_temperatura_30d_c,

        (
            temp_media_c - temp_media_movel_30d_c
        ) / NULLIF(temp_desvio_padrao_30d_c, 0)
            AS zscore_temperatura_30d

    FROM clima_base
),

clima AS (
    SELECT
        data,

        MAX(precipitacao_total_mm)
            FILTER (WHERE cidade = 'patrocinio')
            AS chuva_patrocinio_mm,

        MAX(chuva_acumulada_7d_mm)
            FILTER (WHERE cidade = 'patrocinio')
            AS chuva_7d_patrocinio_mm,

        MAX(chuva_acumulada_30d_mm)
            FILTER (WHERE cidade = 'patrocinio')
            AS chuva_30d_patrocinio_mm,

        MAX(temp_media_c)
            FILTER (WHERE cidade = 'patrocinio')
            AS temp_media_patrocinio_c,

        MAX(anomalia_temperatura_30d_c)
            FILTER (WHERE cidade = 'patrocinio')
            AS anomalia_temp_patrocinio_c,

        MAX(zscore_temperatura_30d)
            FILTER (WHERE cidade = 'patrocinio')
            AS zscore_temp_patrocinio,

        MAX(precipitacao_total_mm)
            FILTER (WHERE cidade = 'franca')
            AS chuva_franca_mm,

        MAX(chuva_acumulada_7d_mm)
            FILTER (WHERE cidade = 'franca')
            AS chuva_7d_franca_mm,

        MAX(chuva_acumulada_30d_mm)
            FILTER (WHERE cidade = 'franca')
            AS chuva_30d_franca_mm,

        MAX(temp_media_c)
            FILTER (WHERE cidade = 'franca')
            AS temp_media_franca_c,

        MAX(anomalia_temperatura_30d_c)
            FILTER (WHERE cidade = 'franca')
            AS anomalia_temp_franca_c,

        MAX(zscore_temperatura_30d)
            FILTER (WHERE cidade = 'franca')
            AS zscore_temp_franca

    FROM clima_metricas
    GROUP BY data
)

SELECT
    c.data_key,
    c.data,
    c.ano,
    c.mes,
    c.trimestre,
    c.ano_mes,
    c.fim_de_semana,

    p.preco_arabica_rs,
    p.preco_robusta_rs,

    p.preco_arabica_rs - p.preco_robusta_rs
        AS spread_arabica_robusta_rs,

    p.variacao_arabica_pct,
    p.variacao_robusta_pct,

    p.media_7d_arabica_rs,
    p.media_30d_arabica_rs,
    p.media_7d_robusta_rs,
    p.media_30d_robusta_rs,

    i.selic_observada_pct,
    i.selic_ultimo_valor_pct,
    i.ipca_mensal_pct,
    i.mes_referencia_ipca,

    cl.chuva_patrocinio_mm,
    cl.chuva_7d_patrocinio_mm,
    cl.chuva_30d_patrocinio_mm,
    cl.temp_media_patrocinio_c,
    cl.anomalia_temp_patrocinio_c,
    cl.zscore_temp_patrocinio,

    cl.chuva_franca_mm,
    cl.chuva_7d_franca_mm,
    cl.chuva_30d_franca_mm,
    cl.temp_media_franca_c,
    cl.anomalia_temp_franca_c,
    cl.zscore_temp_franca,

    p.preco_arabica_rs IS NOT NULL
        AND p.preco_robusta_rs IS NOT NULL
        AS flag_precos_completos,

    cl.temp_media_patrocinio_c IS NOT NULL
        AND cl.temp_media_franca_c IS NOT NULL
        AS flag_clima_completo,

    i.selic_ultimo_valor_pct IS NOT NULL
        AND i.ipca_mensal_pct IS NOT NULL
        AS flag_indicadores_completos

FROM dim_calendario c

LEFT JOIN preco p
    ON c.data = p.data

LEFT JOIN fact_indicadores i
    ON c.data = i.data

LEFT JOIN clima cl
    ON c.data = cl.data

ORDER BY c.data;
