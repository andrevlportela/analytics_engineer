with 

source as (

    select * from {{ ref('inter_aworks__join_razoes') }}

),

renamed as (

select
            salesorderid as pedido_id
            , dsc_razao
            , reasontype
    from source

)

select * from renamed