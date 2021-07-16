WITH PRODUTOS AS (
					  SELECT ROW_NUMBER () OVER ( ORDER BY PRODUCT_ID ) AS SK_PRODUTO --**-- CHAVE SURROGATE
					  		,PRODUCT_ID         AS ID_PRODUTO
					  		,SUPPLIER_ID        AS ID_FORNECEDOR
					  		,CATEGORY_ID	    AS ID_CATEGORIA
					  		
					  		,PRODUCT_NAME 	    AS NOME_PRODUTO
					  		,QUANTITY_PER_UNIT  AS QUANTIDADE_POR_UNIDADE
					  		,UNIT_PRICE 	    AS PRECO_UNITARIO
					  		,UNITS_IN_STOCK 	AS UNIDADES_EM_ESTOQUE
					  		,UNITS_ON_ORDER 	AS UNIDADES_ENCOMENDADAS
					  		,REORDER_LEVEL      AS NIVEL_REABASTECIMENTO
					  		,CASE 
					  		  WHEN ( DISCONTINUED = 1 ) THEN 
					  		    'SIM'
					  		  ELSE
					  		    'NÃO'
					  		 END PRODUTO_DISCONTINUADO
					    FROM {{ source('northiwind_dados_brutos_stitch', 'products' )}} )
SELECT * FROM PRODUTOS