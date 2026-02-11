with 

source as (

    select * from {{ source('aworks', 'person_address') }}

),

renamed as (

    select
        addressid,
        addressline1,
        city,
        stateprovinceid,
        postalcode

    from source

)

select * from renamed