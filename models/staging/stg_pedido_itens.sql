WITH PEDIDO_ITENS AS (  SELECT ORDER_ID 	                           AS ID_PEDIDO
	                          ,PRODUCT_ID      					       AS ID_PRODUTO
							  ,UNIT_PRICE                              AS PRECO_UNITARIO
							  ,QUANTITY                           	   AS QUANTIDADE_TOTAL
							  ,DISCOUNT							   	   AS DESCONTO
					      FROM {{ source('northiwind_dados_brutos_stitch', 'order_details' )}} )
SELECT * FROM PEDIDO_ITENS