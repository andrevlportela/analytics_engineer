with


    razoes_pedidos as (
        select *
        from {{ ref('stg_aworks__sales_salesorderheadersalesreason') }}

        
    )
    , razoes as (
        select *
        from {{ ref('stg_aworks__sales_salesreason') }}
    )


    , joined as (
        select
            razoes_pedidos.salesorderid
            , razoes_pedidos.salesreasonid
            , razoes.dsc_razao
            , razoes.reasontype

        from razoes_pedidos
        inner join razoes 
        on razoes_pedidos.salesreasonid = razoes.salesreasonid

    )

select * from joined