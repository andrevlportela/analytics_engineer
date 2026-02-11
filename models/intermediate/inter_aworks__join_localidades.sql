with


    paises as (
        select *
        from {{ ref('stg_aworks__person_countryregion') }}
     
    )
    , estados as (
        select *
        from {{ ref('stg_aworks__person_stateprovince') }}
    )

    , enderecos as (
        select *
        from {{ ref('stg_aworks__person_address') }}
    )


    , joined as (
        select
            enderecos.addressid
            ,enderecos.addressline1 as endereco
            ,enderecos.city as cidade

            ,estados.stateprovincecode
            ,estados.dsc_estado

            ,paises.countryregioncode
            ,paises.dsc_pais

        from enderecos
        inner join estados
            on enderecos.stateprovinceid=estados.stateprovinceid
        inner join paises
            on estados.countryregioncode=paises.countryregioncode
    )

select * from joined