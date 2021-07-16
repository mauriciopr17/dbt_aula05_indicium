WITH PEDIDOS      AS ( SELECT *
                         FROM {{ ref('stg_orders') }} )
,CLIENTES         AS ( SELECT *
                         FROM {{ ref('dim_clientes') }} )
,COLABORADOR      AS ( SELECT *
                         FROM {{ ref('dim_colaborador') }} ) 
,TRANSPORTADORA   AS ( SELECT *
                         FROM {{ ref('dim_transportadora') }} )
,PRODUTO          AS ( SELECT *
                         FROM {{ ref('dim_produtos') }} )
,FORNECEDOR       AS ( SELECT *
                         FROM {{ ref('dim_fornecedores') }} )
,CATEGORIA        AS ( SELECT *
                         FROM {{ ref('dim_categorias') }} )

,PEDIDO_DETALHES  AS (
                        SELECT ID_PEDIDO
                              
                              ,CLI.SK_CLIENTE          -- CHAVE SURROGATE
                              ,COL.SK_COLABORADOR      -- CHAVE SURROGATE
                              ,TRA.SK_TRANSPORTADORA   -- CHAVE SURROGATE
                              ,PRO.SK_PRODUTO          -- CHAVE SURROGATE
                              ,FO.SK_FORNECEDOR        -- CHAVE SURROGATE
                              ,CAT.SK_CATEGORIA        -- CHAVE SURROGATE

                              ,CLI.ID_CLIENTE  
                              ,COL.ID_COLABORADOR
                              ,TRA.ID_TRANSPORTADORA
                              ,PRO.ID_PRODUTO
                              ,FO.ID_FORNECEDOR
                              
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
                              ,VALOR_POR_ITEM
                              ,QUANTIDADE
                         FROM PEDIDOS             PED
                         LEFT JOIN CLIENTES       CLI ON PED.ID_CLIENTE        = CLI.ID_CLIENTE
                         LEFT JOIN COLABORADOR    COL ON COL.ID_COLABORADOR    = PED.ID_FUNCIONARIO
                         LEFT JOIN TRANSPORTADORA TRA ON TRA.ID_TRANSPORTADORA = PED.ID_TRANSPORTADOR
                         LEFT JOIN PRODUTO        PRO ON PRO.ID_PRODUTO        = PED.ID_PRODUTO
                         LEFT JOIN FORNECEDOR     FO  ON FO.ID_FORNECEDOR      = PRO.ID_FORNECEDOR
                         LEFT JOIN CATEGORIA      CAT ON CAT.ID_CATEGORIA      = PRO.ID_CATEGORIA
                          )
SELECT * FROM PEDIDO_DETALHES