select
        s_name,
        s_address
from
        external('supplier.parquet') supplier,
        external('nation.parquet') nation
where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        external('partsupp.parquet') partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        external('part.parquet') part
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        external('lineitem.parquet') lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1994-01-01'
                                        and l_shipdate < date '1994-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
order by
        s_name;
