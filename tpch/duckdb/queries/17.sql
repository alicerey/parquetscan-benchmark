select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
        'lineitem.parquet' lineitem,
        'part.parquet' part
where
        p_partkey = l_partkey
        and p_brand = 'Brand#23'
        and p_container = 'MED BOX'
        and l_quantity < (
                select
                        0.2 * avg(l_quantity)
                from
                        'lineitem.parquet' lineitem
                where
                        l_partkey = p_partkey
        );
