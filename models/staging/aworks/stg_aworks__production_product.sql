with 

source as (

    select * from {{ source('aworks', 'production_product') }}

),

renamed as (

    select
        productid,
        name,
        productnumber,
        makeflag,
        finishedgoodsflag,
        safetystocklevel,
        reorderpoint,
        standardcost,
        listprice,
        size,
        sizeunitmeasurecode,
        weightunitmeasurecode,
        weight,
        daystomanufacture,
        productline,
        class,
        style,
        productsubcategoryid,
        productmodelid,
        sellstartdate,
        sellenddate,
        discontinueddate

    from source

)

select * from renamed