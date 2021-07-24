WITH STG_FORNECEDOR AS ( SELECT *
                            FROM {{ ref('stg_fornecedores') }} )

SELECT * FROM STG_FORNECEDOR