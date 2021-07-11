WITH STG_TRANSPORTADORA AS ( SELECT *
                               FROM {{ ref('stg_transportadora') }} )

SELECT * FROM STG_TRANSPORTADORA