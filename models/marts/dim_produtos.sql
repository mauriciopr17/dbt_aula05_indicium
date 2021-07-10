WITH STG_PRODUTOS AS ( SELECT *
                            FROM {{ ref('stg_produtos') }} )

SELECT * FROM STG_PRODUTOS