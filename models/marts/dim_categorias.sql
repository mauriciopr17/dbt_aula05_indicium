WITH STG_CATEGORIAS AS ( SELECT *
                         FROM {{ ref('stg_categorias') }} )
SELECT * FROM STG_CATEGORIAS