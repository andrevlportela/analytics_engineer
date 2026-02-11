with 

source as (

    select * from {{ source('aworks', 'person_person') }}

),

renamed as (

    select
        businessentityid,
        persontype,
        namestyle,
        title,
        firstname,
        middlename,
        lastname,
        suffix,
        emailpromotion

    from source

)

select * from renamed