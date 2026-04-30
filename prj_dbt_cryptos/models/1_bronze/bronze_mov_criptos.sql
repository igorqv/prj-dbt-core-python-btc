--import
WITH source AS (
    SELECT 
        date,
        action,
        symbol,
        quantity,
        price
    FROM 
        {{ source('dbcrypto_tf00', 'action_cripto') }}
),
--renamed
renamed AS (
    SELECT 
        cast(date as date) AS data_trade,
        action as tipo_trade,
        symbol as nome_crypto,
        quantity as quantidade_trade,
        price as valor_trade
    FROM 
        source
)
--select
SELECT 
    data_trade,
    tipo_trade,
    nome_crypto,
    quantidade_trade,
    valor_trade
FROM 
    renamed