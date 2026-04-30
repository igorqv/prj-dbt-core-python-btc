--import
WITH source AS (
    SELECT 
        "Date",
        "Close",
        simbolo
    FROM 
        {{ source('dbcrypto_tf00', 'tbcryptos') }}
),
--renamed
renamed AS (
    SELECT 
        cast("Date" as date) AS data_fechamento,
        "Close" AS valor_fechamento,
        simbolo as nome_crypto
    FROM 
        source
)
--select
SELECT 
    data_fechamento,
    valor_fechamento,
    nome_crypto
FROM 
    renamed