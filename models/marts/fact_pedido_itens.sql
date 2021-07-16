WITH STG_PEDIDO_ITENS AS ( SELECT *
                            FROM {{ ref('stg_pedido_itens') }} )

SELECT * FROM STG_PEDIDO_ITENS