SELECT COLABORADOR AS (
                        SELECT ROW_NUMBER() OVER ( ORDER BY E.EMPLOYEE_ID ) AS SK_COLABORADOR -- CHAVE PRIMARIA
                            ,E.EMPLOYEE_ID              AS ID_COLABORADOR
                            ,E.FIRST_NAME               AS NOME_COLABORADOR
                            ,E.LAST_NAME 	            AS SOBRENOME_COLABORADOR
                            ,E.TITLE 	                AS CARGO
                            ,E.BIRTH_DATE               AS DATA_NASCIMENTO
                            ,E.HIRE_DATE 	            AS DATA_ADMISSAO
                            ,E.ADDRESS || E."EXTENSION" AS ENDERECO
                            ,E.CITY 	     		    AS CIDADE
                            ,E.POSTAL_CODE 			    AS CODIGO_POSTAL
                            ,E.COUNTRY 	 		        AS PAIS
                            ,E.HOME_PHONE  		        AS TELEFONE_RESIDENCIA
                            ,E.NOTES 	     		    AS OBSERVACAO
                        FROM {{ source('northiwind_dados_brutos_stitch', 'employees' )}} )
SELECT * FROM COLABORADOR                        
