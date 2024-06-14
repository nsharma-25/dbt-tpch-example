with source as (

    select * from {{ source('tpch', 'partsupp') }}

),

final as (

    select

        ps_partkey as part_key,
        ps_suppkey as supplier_key,
        ps_availqty as available_quantity,
        ps_supplycost as cost,
        ps_comment as comment

    from

        source
) 

select * from final