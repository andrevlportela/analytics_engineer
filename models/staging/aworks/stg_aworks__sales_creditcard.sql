with 

source as (

    select * from {{ source('aworks', 'sales_creditcard') }}

),

renamed as (

    select
        creditcardid,
        cardtype,
        cardnumber

    from source

)

select * from renamed