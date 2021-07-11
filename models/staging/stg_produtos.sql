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
					    FROM {{ source('northiwind_dados_brutos_stitch', 'products' )}} ) )
,FORNECEDORES AS (    SELECT ROW_NUMBER() OVER ( ORDER BY SUPPLIER_ID )  AS SK_FORNECEDORES --**-- CHAVE SURROGATE
							,SUPPLIER_ID        AS ID_FORNECEDOR
							,COMPANY_NAME	    AS NOME_EMPRESA
							,CONTACT_NAME       AS NOME_CONTATO
							,CONTACT_TITLE      AS CARGO_CONTATO
							,ADDRESS            AS ENDERECO
							,CITY	            AS CIDADE
							,POSTAL_CODE		AS CODIGO_POSTAL
							,COUNTRY		    AS PAIS
							,PHONE	    		AS TELEFONE
							,FAX			    AS TELEFONE_FAX
							,HOMEPAGE	        AS WEB_SITE					
	                     FROM {{ source('northiwind_dados_brutos_stitch', 'suppliers' )}} )
,PRODUTOS_DETALHES AS (   SELECT P.SK_PRODUTO --**-- CHAVE SURROGATE
						  		,P.ID_PRODUTO
						  		,F.SK_FORNECEDORES --**-- CHAVE SURROGATE
						  		,F.ID_FORNECEDOR
						  		,P.ID_CATEGORIA
						  		
						  		,P.NOME_PRODUTO
						  		,P.QUANTIDADE_POR_UNIDADE
						  		,P.PRECO_UNITARIO
						  		,P.UNIDADES_EM_ESTOQUE
						  		,P.UNIDADES_ENCOMENDADAS
						  		,P.NIVEL_REABASTECIMENTO
						  		,P.PRODUTO_DISCONTINUADO
						  		
								,F.NOME_EMPRESA
								,F.NOME_CONTATO
								,F.CARGO_CONTATO
								,F.ENDERECO
								,F.CIDADE
								,F.CODIGO_POSTAL
								,F.PAIS
								,F.TELEFONE
								,F.TELEFONE_FAX
								,F.WEB_SITE	
				         FROM PRODUTOS P
				       LEFT JOIN FORNECEDORES F ON F.ID_FORNECEDOR = P.ID_FORNECEDOR)
SELECT * FROM PRODUTOS_DETALHES