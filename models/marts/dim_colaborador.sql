WITH STG_COLABORADOR AS ( SELECT *
                            FROM {{ ref('stg_colaborador') }} )

SELECT * FROM STG_COLABORADOR