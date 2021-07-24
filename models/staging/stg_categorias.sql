WITH CATEGORIAS AS (
						SELECT ROW_NUMBER() OVER ( ORDER BY CATEGORY_ID ) AS SK_CATEGORIA --**-- CHAVE SURROGATE
						      ,CATEGORY_ID     AS ID_CATEGORIA
							  ,CATEGORY_NAME   AS NOME_CATEGORIA
							  ,DESCRIPTION 	   AS DESCRICAO_CATEGORIA
						 FROM {{ source('northiwind_dados_brutos_stitch', 'categories' )}} )
SELECT * FROM CATEGORIAS						 
