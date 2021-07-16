WITH TEST_PEDIDOS AS ( SELECT COUNT( DISTINCT ID_PEDIDO ) QTD
                         FROM {{ref('fact_pedidos')}}
                       HAVING COUNT( DISTINCT ID_PEDIDO ) <> 830 )
SELECT * FROM TEST_PEDIDOS
