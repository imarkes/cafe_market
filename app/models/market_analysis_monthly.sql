CREATE OR REPLACE TABLE cafe_market_analysis_monthly AS

WITH preco_mensal AS (
    SELECT
        date_trunc('month', data)::DATE AS mes_referencia,
        tipo_cafe,

        AVG(preco_rs_saca) AS preco_medio_rs,
        MIN(preco_rs_saca) AS preco_minimo_rs,
        MAX(preco_rs_saca) AS preco_maximo_rs,

        ARG_MIN(preco_rs_saca, data) AS preco_inicio_mes_rs,
        ARG_MAX(preco_rs_saca, data) AS preco_fim_mes_rs

    FROM fact_preco_cafe
    GROUP BY
        date_trunc('month', data)::DATE,
        tipo_cafe
),

preco AS (
    SELECT
        mes_referencia,

        MAX(preco_medio_rs)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS preco_medio_arabica_rs,

        MAX(preco_medio_rs)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS preco_medio_robusta_rs,

        MAX(preco_inicio_mes_rs)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS preco_inicio_arabica_rs,

        MAX(preco_fim_mes_rs)
            FILTER (WHERE tipo_cafe = 'arabica')
            AS preco_fim_arabica_rs,

        MAX(preco_inicio_mes_rs)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS preco_inicio_robusta_rs,

        MAX(preco_fim_mes_rs)
            FILTER (WHERE tipo_cafe = 'robusta')
            AS preco_fim_robusta_rs

    FROM preco_mensal
    GROUP BY mes_referencia
),

clima_mensal AS (
    SELECT
        date_trunc('month', f.data)::DATE AS mes_referencia,
        e.cidade,

        SUM(f.precipitacao_total_mm)
            AS precipitacao_total_mes_mm,

        AVG(f.temp_media_c)
            AS temperatura_media_mes_c,

        AVG(f.umidade_rel_media)
            AS umidade_media_mes,

        COUNT(*) FILTER (
            WHERE f.precipitacao_total_mm IS NULL
               OR f.temp_media_c IS NULL
        ) AS dias_com_dados_incompletos

    FROM fact_clima f

    INNER JOIN dim_estacao e
        ON f.codigo_estacao = e.codigo_estacao

    GROUP BY
        date_trunc('month', f.data)::DATE,
        e.cidade
),

clima AS (
    SELECT
        mes_referencia,

        MAX(precipitacao_total_mes_mm)
            FILTER (WHERE cidade = 'patrocinio')
            AS chuva_mes_patrocinio_mm,

        MAX(temperatura_media_mes_c)
            FILTER (WHERE cidade = 'patrocinio')
            AS temp_media_mes_patrocinio_c,

        MAX(dias_com_dados_incompletos)
            FILTER (WHERE cidade = 'patrocinio')
            AS dias_incompletos_patrocinio,

        MAX(precipitacao_total_mes_mm)
            FILTER (WHERE cidade = 'franca')
            AS chuva_mes_franca_mm,

        MAX(temperatura_media_mes_c)
            FILTER (WHERE cidade = 'franca')
            AS temp_media_mes_franca_c,

        MAX(dias_com_dados_incompletos)
            FILTER (WHERE cidade = 'franca')
            AS dias_incompletos_franca

    FROM clima_mensal
    GROUP BY mes_referencia
),

indicadores AS (
    SELECT
        date_trunc('month', data)::DATE AS mes_referencia,

        AVG(selic_ultimo_valor_pct)
            AS selic_media_mes_pct,

        MAX(ipca_mensal_pct)
            AS ipca_mensal_pct

    FROM fact_indicadores
    GROUP BY date_trunc('month', data)::DATE
)

SELECT
    p.mes_referencia,
    EXTRACT(year FROM p.mes_referencia)::INTEGER AS ano,
    EXTRACT(month FROM p.mes_referencia)::INTEGER AS mes,

    p.preco_medio_arabica_rs,
    p.preco_medio_robusta_rs,

    p.preco_medio_arabica_rs -
    p.preco_medio_robusta_rs
        AS spread_medio_arabica_robusta_rs,

    CASE
        WHEN p.preco_inicio_arabica_rs IS NULL
          OR p.preco_inicio_arabica_rs = 0
        THEN NULL
        ELSE 100 * (
            p.preco_fim_arabica_rs /
            p.preco_inicio_arabica_rs - 1
        )
    END AS variacao_mensal_arabica_pct,

    CASE
        WHEN p.preco_inicio_robusta_rs IS NULL
          OR p.preco_inicio_robusta_rs = 0
        THEN NULL
        ELSE 100 * (
            p.preco_fim_robusta_rs /
            p.preco_inicio_robusta_rs - 1
        )
    END AS variacao_mensal_robusta_pct,

    i.selic_media_mes_pct,
    i.ipca_mensal_pct,

    c.chuva_mes_patrocinio_mm,
    c.temp_media_mes_patrocinio_c,
    c.dias_incompletos_patrocinio,

    c.chuva_mes_franca_mm,
    c.temp_media_mes_franca_c,
    c.dias_incompletos_franca

FROM preco p

LEFT JOIN indicadores i
    ON p.mes_referencia = i.mes_referencia

LEFT JOIN clima c
    ON p.mes_referencia = c.mes_referencia

ORDER BY p.mes_referencia;
