WITH FORNECEDORES AS (    SELECT ROW_NUMBER() OVER ( ORDER BY SUPPLIER_ID )  AS SK_FORNECEDOR --**-- CHAVE SURROGATE
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

SELECT * FROM FORNECEDORES