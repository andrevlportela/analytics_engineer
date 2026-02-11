with 

source as (

    select * from {{ source('aworks', 'sales_salesreason') }}

),

renamed as (

    select
        salesreasonid,
        name as dsc_razao,
        reasontype

    from source

)

select * from renamed