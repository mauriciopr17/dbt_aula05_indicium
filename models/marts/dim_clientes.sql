WITH STG_CLIENTES AS ( SELECT *
                         FROM {{ ref('stg_clientes') }} )

SELECT * FROM STG_CLIENTES