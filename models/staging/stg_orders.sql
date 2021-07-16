WITH PEDIDOS AS (   SELECT    ORDER_ID         AS ID_PEDIDO
					        , EMPLOYEE_ID 	   AS ID_FUNCIONARIO
					        , ORDER_DATE       AS DATA_PEDIDO
					        , CUSTOMER_ID      AS ID_CLIENTE
					        , SHIP_REGION      AS REGIAO_ENTREGA
					        , SHIPPED_DATE     AS DATA_EXPEDICAO
					        , SHIP_COUNTRY     AS PAIS_ENTREGA
					        , SHIP_NAME        AS NOME_ENTREGA
					        , SHIP_POSTAL_CODE AS CEP_ENTREGA
					        , SHIP_CITY 	   AS CIDADE_ENTREGA
					        , FREIGHT 	       AS FRETE
					        , SHIP_VIA 		   AS ID_TRANSPORTADOR
					        , SHIP_ADDRESS     AS ENDERECO_ENTREGA
					        , REQUIRED_DATE    AS DATA_PREVISTA
					    FROM {{ source('northiwind_dados_brutos_stitch', 'orders' )}} )

,PEDIDO_ITENS AS (  SELECT ORDER_ID 	                               AS ID_PEDIDO_ITEM
	                      ,PRODUCT_ID      					           AS ID_PRODUTO
						  ,QUANTITY                     	       	   AS QUANTIDADE
						  ,(UNIT_PRICE * ( 1 - DISCOUNT ) * QUANTITY ) AS VALOR_POR_ITEM
					    FROM {{ source('northiwind_dados_brutos_stitch', 'order_details' )}} )
,PEDIDO_DETALHES AS ( SELECT PEDIDOS.ID_PEDIDO
							,ID_CLIENTE
					        ,ID_FUNCIONARIO -- PRECISO TROCAR POR SK
					        ,ID_TRANSPORTADOR -- PRECISO TROCAR POR SK
					        ,ID_PRODUTO
					        ,DATA_PEDIDO
					        ,REGIAO_ENTREGA
					        ,DATA_EXPEDICAO
					        ,PAIS_ENTREGA
					        ,NOME_ENTREGA
					        ,CEP_ENTREGA
					        ,CIDADE_ENTREGA
					        ,FRETE
					        ,ENDERECO_ENTREGA
					        ,DATA_PREVISTA
					        ,QUANTIDADE
					        ,VALOR_POR_ITEM
					   FROM PEDIDOS
					  LEFT JOIN PEDIDO_ITENS ON PEDIDOS.ID_PEDIDO = PEDIDO_ITENS.ID_PEDIDO_ITEM )
SELECT * FROM PEDIDO_DETALHES