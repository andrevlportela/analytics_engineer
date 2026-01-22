with 

source_orders_details as (

    select * from {{ source('nwind', 'order_details') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['order_id','product_id'])}} as order_item_sk
        ,cast(order_id as int) as order_fk
        ,cast(product_id as int) as product_fk
        ,cast(unit_price as float) as unit_price
        ,cast(quantity as int) as quantity
        ,cast(discount as float) as discount_pct

    from source_orders_details

)

select * from renamed