with 

source as (

    select * from {{ source('aworks', 'sales_salesorderheader') }}

),

renamed as (

    select
        salesorderid as pedido_id,
        cast(orderdate as date) as data_pedido,
        cast(shipdate as date) as data_envio,
        status,
        customerid,
        billtoaddressid as id_ender_cobranca,
        shiptoaddressid as id_ender_envio,
        shipmethodid,
        creditcardid,
        currencyrateid,
        subtotal as vlr_subtotal,
        taxamt as vlr_total_imposto,
        freight as vlr_total_frete,
        totaldue as vlr_total
    from source

)

select * from renamed