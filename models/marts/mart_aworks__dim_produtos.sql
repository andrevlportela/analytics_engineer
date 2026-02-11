with 

source as (

    select * from {{ ref('stg_aworks__production_product') }}

),

renamed as (

    select
        productid,
        name as dsc_produto,
        productnumber,
        safetystocklevel

    from source

)

select * from renamed