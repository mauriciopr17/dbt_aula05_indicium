WITH PEDIDOS AS ( SELECT *
                    FROM {{ ref('stg_orders') }} )
,CLIENTES AS ( SELECT *
                 FROM {{ ref('dim_clientes') }} )
,COLABORADOR AS ( SELECT *
                    FROM {{ ref('dim_colaborador') }} ) 
,TRANSPORTADORA AS ( SELECT *
                       FROM {{ ref('dim_transportadora') }} )

,PEDIDO_DETALHES AS (
                        SELECT ID_PEDIDO
                              ,CLI.SK_CLIENTE --CHAVE AUTO-INCREMENTAL
                              ,COL.SK_COLABORADOR -- CHAVE SURROGATE
                              ,TRA.SK_TRANSPORTADORA -- CHAVE SURROGATE
                              ,COL.NOME_COLABORADOR
                              ,COL.SOBRENOME_COLABORADOR
                              ,TRA.NOME_TRANSPORTADORA
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
                              ,QUANTIDADE_TOTAL
                              ,VALOR_FATURADO
                              ,QUANTIDADE_ITENS
                         FROM PEDIDOS             PED
                         LEFT JOIN CLIENTES       CLI ON PED.ID_CLIENTE        = CLI.ID_CLIENTE
                         LEFT JOIN COLABORADOR    COL ON COL.ID_COLABORADOR    = PED.ID_FUNCIONARIO
                         LEFT JOIN TRANSPORTADORA TRA ON TRA.ID_TRANSPORTADORA = PED.ID_TRANSPORTADOR )

SELECT * FROM PEDIDO_DETALHES