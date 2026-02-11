with 

source as (

    select * from {{ source('aworks', 'sales_salesorderdetail') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid','salesorderdetailid','productid'])}} as item_unico_pk,
        salesorderid as pedido_id,
        salesorderdetailid as item_id,
        productid,
        orderqty as quantidade,
        specialofferid,
        unitprice as vlr_unitario,
        unitpricediscount as vlr_desconto

    from source

)

select * from renamed