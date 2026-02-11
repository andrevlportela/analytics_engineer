with 

source as (

    select * from {{ source('aworks', 'person_countryregion') }}

),

renamed as (

    select
        countryregioncode,
        name as dsc_pais

    from source

)

select * from renamed