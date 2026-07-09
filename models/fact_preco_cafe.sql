CREATE OR REPLACE TABLE fact_preco_cafe AS

SELECT
    data,
    'arabica' AS tipo_cafe,
    a_vista_r AS preco_rs_saca,
    a_vista_us AS preco_usd_saca
FROM read_parquet('{arabica}')

UNION ALL

SELECT
    data,
    'robusta' AS tipo_cafe,
    a_vista_r AS preco_rs_saca,
    a_vista_us AS preco_usd_saca
FROM read_parquet('{robusta}');
