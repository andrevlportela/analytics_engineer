with 

cliente as (

    select * from {{ ref('stg_aworks__person_person') }}

),

cliente_venda as (

    select * from {{ ref('stg_aworks__sales_customer') }}

),

renamed as (

    select
        cliente_venda.customerid,
        cliente.businessentityid as personid,
        concat(ifnull(cliente.firstname,''),' ',ifnull(cliente.middlename,''),' ',ifnull(cliente.lastname,''),' ',ifnull(cliente.suffix,'')) as nome_completo


    from cliente
    inner join cliente_venda
    on cliente.businessentityid = cliente_venda.personid 
)

select * from renamed