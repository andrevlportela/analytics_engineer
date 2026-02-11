with 

source as (

    select * from {{ source('aworks', 'person_stateprovince') }}

),

renamed as (

    select
        stateprovinceid,
        stateprovincecode,
        countryregioncode,
        isonlystateprovinceflag,
        name as dsc_estado

    from source

)

select * from renamed