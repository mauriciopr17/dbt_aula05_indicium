WITH COLABORADOR AS (
                        SELECT ROW_NUMBER() OVER ( ORDER BY EMPLOYEE_ID ) AS SK_COLABORADOR -- CHAVE PRIMARIA
                            ,EMPLOYEE_ID            AS ID_COLABORADOR
                            ,FIRST_NAME             AS NOME_COLABORADOR
                            ,LAST_NAME 	            AS SOBRENOME_COLABORADOR
                            ,TITLE 	                AS CARGO
                            ,BIRTH_DATE             AS DATA_NASCIMENTO
                            ,HIRE_DATE 	            AS DATA_ADMISSAO
                            ,ADDRESS || "EXTENSION" AS ENDERECO
                            ,CITY 	     		    AS CIDADE
                            ,POSTAL_CODE 			AS CODIGO_POSTAL
                            ,COUNTRY 	 		    AS PAIS
                            ,HOME_PHONE  		    AS TELEFONE_RESIDENCIA
                            ,NOTES 	     		    AS OBSERVACAO
                        FROM {{ source('northiwind_dados_brutos_stitch', 'employees' )}} )
SELECT * FROM COLABORADOR                        
