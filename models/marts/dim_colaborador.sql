WITH SGT_COLABORADOR AS ( SELECT *
                            FROM {{ ref('stg_colaborador') }} )

SELECT * FROM SGT_COLABORADOR