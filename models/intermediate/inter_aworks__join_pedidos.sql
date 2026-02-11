with

    inter_pedidos_items as (
        select *
        from {{ ref('stg_aworks__sales_salesorderdetail') }}

    )
    , inter_pedidos as (
        select *
        from {{ ref('stg_aworks__sales_salesorderheader') }}
    )

    , inter_cartoes as (
        select *
        from {{ ref('stg_aworks__sales_creditcard') }}
    )

    , joined as (
        select
            inter_pedidos.pedido_id,
            inter_pedidos.data_pedido,
            inter_pedidos.data_envio,
            inter_pedidos.status,
            inter_pedidos.customerid,
            inter_pedidos.id_ender_cobranca,
            inter_pedidos.id_ender_envio,
            inter_pedidos.shipmethodid,
            inter_pedidos.creditcardid,
            inter_pedidos.currencyrateid,
            inter_pedidos.vlr_subtotal,
            inter_pedidos.vlr_total_imposto,
            inter_pedidos.vlr_total_frete,
            inter_pedidos.vlr_total,

            inter_pedidos_items.item_unico_pk,
            inter_pedidos_items.item_id,
            inter_pedidos_items.productid,
            inter_pedidos_items.quantidade,
            inter_pedidos_items.specialofferid,
            inter_pedidos_items.vlr_unitario,
            inter_pedidos_items.vlr_desconto,

            inter_cartoes.cardtype
            
        from inter_pedidos_items
        inner join inter_pedidos 
            on inter_pedidos_items.pedido_id = inter_pedidos.pedido_id
        inner join inter_cartoes
            on inter_pedidos.creditcardid =  inter_cartoes.creditcardid

    )

select * from joined