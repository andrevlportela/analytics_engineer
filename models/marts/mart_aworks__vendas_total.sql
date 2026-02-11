with

    int_sales as 
    (
        select *
        from {{ ref('inter_aworks__join_pedidos') }}
    )

    , int_localidade as 
    (
        select *
        from {{ ref('inter_aworks__join_localidades') }}
    )

    , metrics as (
        select
            int_sales.item_unico_pk
            , year(int_sales.data_pedido) as Ano
            , month(int_sales.data_pedido) as Mes 
            , int_sales.pedido_id
            , int_sales.item_id
            , int_sales.data_pedido
            , int_sales.status
            , int_sales.customerid
            , int_sales.id_ender_cobranca
            , int_sales.cardtype
            , int_sales.productid
            , int_sales.vlr_unitario
            , int_sales.quantidade
            , int_sales.vlr_desconto
            , int_sales.vlr_unitario * int_sales.quantidade as vlr_total_item
            , int_sales.vlr_unitario * (1 - int_sales.vlr_desconto) * int_sales.quantidade as vlr_total_com_desconto
            , cast(
                (int_sales.vlr_total_frete / count(*) over (partition by int_sales.pedido_id))
            as numeric(18,2)) as frete_alocado
            , case
                when int_sales.vlr_desconto > 0 then true
                else false
            end as tem_desconto
            , int_localidade.dsc_cidade
            , int_localidade.dsc_estado
            , int_localidade.dsc_pais
        from int_sales
        inner join int_localidade
        on int_sales.id_ender_cobranca = int_localidade.addressid
    )
select *
from metrics