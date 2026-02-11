with
    -- import models
    int_ticket as 
    (
        select *
        from {{ ref('inter_aworks__join_pedidos') }}
    )

    , int_localidade as 
    (
        select *
        from {{ ref('inter_aworks__join_localidades') }}
    )

    , int_produto as 
    (
        select *
        from {{ ref('mart_aworks__dim_produtos') }}
    )
    
    , metrics as (
        select
            year(int_ticket.data_pedido) as Ano
            , month(int_ticket.data_pedido) as Mes 
            , int_localidade.dsc_cidade
            , int_localidade.dsc_estado
            , int_localidade.dsc_pais
            , int_ticket.productid
            , int_produto.dsc_produto
            , count(distinct int_ticket.pedido_id) numero_de_pedidos
            , sum(int_ticket.vlr_unitario * int_ticket.quantidade) as faturamento_bruto
            , sum(int_ticket.vlr_unitario * (1 - int_ticket.vlr_desconto) * int_ticket.quantidade) as faturamento_com_desconto
            , (
                sum(int_ticket.vlr_unitario * (1 - int_ticket.vlr_desconto) * int_ticket.quantidade) /
                count(distinct int_ticket.pedido_id)
            ) as ticket_medio  

        from int_ticket
        inner join int_localidade
        on int_ticket.id_ender_cobranca = int_localidade.addressid
        inner join int_produto
        on int_ticket.productid = int_produto.productid

        group by
            year(int_ticket.data_pedido)
            , month(int_ticket.data_pedido) 
            , int_localidade.dsc_cidade
            , int_localidade.dsc_estado
            , int_localidade.dsc_pais
            , int_ticket.productid
            , int_produto.dsc_produto
           
    )
select *
from metrics