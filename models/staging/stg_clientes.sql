WITH CLIENTES AS ( 
                    SELECT ROW_NUMBER() OVER ( ORDER BY CUSTOMER_ID ) SK_CLIENTE --**-- CHAVE SURROGATE
                         ,CUSTOMER_ID   AS ID_CLIENTE       
                         ,CONTACT_NAME  AS NOME_CONTATO    
                         ,CONTACT_TITLE AS CARGO       
                         ,PHONE         AS TELEFONE    
                         ,COMPANY_NAME  AS NOME_EMPRESA    
                         ,CITY          AS CIDADE      
                         ,REGION        AS REGIAO      
                         ,COUNTRY       AS PAIS        
                         ,POSTAL_CODE   AS CEP     
                         ,ADDRESS       AS ENDERECO 
                    FROM {{ source('northiwind_dados_brutos_stitch', 'customers')}} )
SELECT * FROM CLIENTES