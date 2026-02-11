with 

source as (

    select * from {{ source('aworks', 'sales_salesorderheadersalesreason') }}

),

renamed as (

    select
        salesorderid,
        max(salesreasonid) as salesreasonid

    from source
    group by salesorderid

)

select * from renamed