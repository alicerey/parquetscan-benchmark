#!/usr/bin/python3
import pyarrow.parquet as pq
from pyarrow import csv
import pyarrow as pa
import sys

if len(sys.argv) != 3:
    print("Please pass the source path of the TPC-H CSV files and the destination path where the Parquet files should be stored")
    sys.exit()

source_path = sys.argv[1]
destination_path = sys.argv[2]

part_column_names = ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment']
supplier_column_names = ['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctbal','s_comment']
partsupp_column_names = ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment']
customer_column_names = ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment']
orders_column_names = ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment']
lineitem_column_names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment']
nation_column_names = ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment']
region_column_names = ['r_regionkey', 'r_name', 'r_comment']

part_column_types = {
    'p_partkey': pa.int32(),
    'p_name': pa.string(),
    'p_mfgr': pa.string(),
    'p_brand': pa.string(),
    'p_type': pa.string(),
    'p_size': pa.int32(),
    'p_container': pa.string(),
    'p_retailprice': pa.decimal128(12,2),
    'p_comment': pa.string()
}
supplier_column_types = {
    's_suppkey': pa.int32(),
    's_name': pa.string(),
    's_address': pa.string(),
    's_nationkey': pa.int32(),
    's_phone': pa.string(),
    's_acctbal': pa.decimal128(12,2),
    's_comment': pa.string()
}
partsupp_column_types = {
    'ps_partkey': pa.int32(),
    'ps_suppkey': pa.int32(),
    'ps_availqty': pa.int32(),
    'ps_supplycost': pa.decimal128(12,2),
    'ps_comment': pa.string()
}
customer_column_types = {
    'c_custkey': pa.int32(),
    'c_name': pa.string(),
    'c_address': pa.string(),
    'c_nationkey': pa.int32(),
    'c_phone': pa.string(),
    'c_acctbal': pa.decimal128(12,2),
    'c_mktsegment': pa.string(),
    'c_comment': pa.string()
}
orders_column_types = {
    'o_orderkey': pa.int32(),
    'o_custkey': pa.int32(),
    'o_orderstatus': pa.string(),
    'o_totalprice': pa.decimal128(12,2),
    'o_orderdate': pa.date32(),
    'o_orderpriority': pa.string(),
    'o_clerk': pa.string(),
    'o_shippriority': pa.string(),
    'o_comment': pa.string()
}
lineitem_column_types = {
    'l_orderkey': pa.int32(),
    'l_partkey': pa.int32(),
    'l_suppkey': pa.int32(),
    'l_linenumber': pa.int32(),
    'l_quantity': pa.decimal128(12,2),
    'l_extendedprice': pa.decimal128(12,2),
    'l_discount': pa.decimal128(12,2),
    'l_tax': pa.decimal128(12,2),
    'l_returnflag': pa.string(),
    'l_linestatus': pa.string(),
    'l_shipdate': pa.date32(),
    'l_commitdate': pa.date32(),
    'l_receiptdate': pa.date32(),
    'l_shipinstruct': pa.string(),
    'l_shipmode': pa.string(),
    'l_comment': pa.string()
}
nation_column_types = {
    'n_nationkey': pa.int32(),
    'n_name': pa.string(),
    'n_regionkey': pa.int32(),
    'n_comment': pa.string()
}
region_column_types = {
    'r_regionkey': pa.int32(),
    'r_name': pa.string(),
    'r_comment': pa.string()
}

part_schema = [
    pa.field('p_partkey', pa.int32(), False),
    pa.field('p_name', pa.string(), False),
    pa.field('p_mfgr', pa.string(), False),
    pa.field('p_brand', pa.string(), False),
    pa.field('p_type', pa.string(), False),
    pa.field('p_size', pa.int32(), False),
    pa.field('p_container', pa.string(), False),
    pa.field('p_retailprice', pa.decimal128(12,2), False),
    pa.field('p_comment', pa.string(), False)
]
supplier_schema = [
    pa.field('s_suppkey', pa.int32(), False),
    pa.field('s_name', pa.string(), False),
    pa.field('s_address', pa.string(), False),
    pa.field('s_nationkey', pa.int32(), False),
    pa.field('s_phone', pa.string(), False),
    pa.field('s_acctbal', pa.decimal128(12,2), False),
    pa.field('s_comment', pa.string(), False)
]
partsupp_schema = [
    pa.field('ps_partkey', pa.int32(), False),
    pa.field('ps_suppkey', pa.int32(), False),
    pa.field('ps_availqty', pa.int32(), False),
    pa.field('ps_supplycost', pa.decimal128(12,2), False),
    pa.field('ps_comment', pa.string(), False)
]
customer_schema = [
    pa.field('c_custkey', pa.int32(), False),
    pa.field('c_name', pa.string(), False),
    pa.field('c_address', pa.string(), False),
    pa.field('c_nationkey', pa.int32(), False),
    pa.field('c_phone', pa.string(), False),
    pa.field('c_acctbal', pa.decimal128(12,2), False),
    pa.field('c_mktsegment', pa.string(), False),
    pa.field('c_comment', pa.string(), False)
]
orders_schema = [
    pa.field('o_orderkey', pa.int32(), False),
    pa.field('o_custkey', pa.int32(), False),
    pa.field('o_orderstatus', pa.string(), False),
    pa.field('o_totalprice', pa.decimal128(12,2), False),
    pa.field('o_orderdate', pa.date32(), False),
    pa.field('o_orderpriority', pa.string(), False),
    pa.field('o_clerk', pa.string(), False),
    pa.field('o_shippriority', pa.string(), False),
    pa.field('o_comment', pa.string(), False)
]
lineitem_schema = [
    pa.field('l_orderkey', pa.int32(), False),
    pa.field('l_partkey', pa.int32(), False),
    pa.field('l_suppkey', pa.int32(), False),
    pa.field('l_linenumber', pa.int32(), False),
    pa.field('l_quantity', pa.decimal128(12,2), False),
    pa.field('l_extendedprice', pa.decimal128(12,2), False),
    pa.field('l_discount', pa.decimal128(12,2), False),
    pa.field('l_tax', pa.decimal128(12,2), False),
    pa.field('l_returnflag', pa.string(), False),
    pa.field('l_linestatus', pa.string(), False),
    pa.field('l_shipdate', pa.date32(), False),
    pa.field('l_commitdate', pa.date32(), False),
    pa.field('l_receiptdate', pa.date32(), False),
    pa.field('l_shipinstruct', pa.string(), False),
    pa.field('l_shipmode', pa.string(), False),
    pa.field('l_comment', pa.string(), False)
]
nation_schema = [
    pa.field('n_nationkey', pa.int32(), False),
    pa.field('n_name', pa.string(), False),
    pa.field('n_regionkey', pa.int32(), False),
    pa.field('n_comment', pa.string(), False)
]
region_schema = [
    pa.field('r_regionkey', pa.int32(), False),
    pa.field('r_name', pa.string(), False),
    pa.field('r_comment', pa.string(), False)
]

def generateParquetFile(filename, column_names, column_types, schema):
    path = source_path + "/" + filename + ".tbl"
    table = csv.read_csv(path, parse_options=csv.ParseOptions(delimiter="|"), read_options=csv.ReadOptions(column_names=column_names),
        convert_options=csv.ConvertOptions(column_types=lineitem_column_types)
    )
    table = table.cast(pa.schema(schema))
    pq.write_table(table, destination_path + "/" + filename + ".parquet")

generateParquetFile("part", part_column_names, part_column_types, part_schema)
generateParquetFile("supplier", supplier_column_names, supplier_column_types, supplier_schema)
generateParquetFile("partsupp", partsupp_column_names, partsupp_column_types, partsupp_schema)
generateParquetFile("customer", customer_column_names, customer_column_types, customer_schema)
generateParquetFile("orders", orders_column_names, orders_column_types, orders_schema)
generateParquetFile("lineitem", lineitem_column_names, lineitem_column_types, lineitem_schema)
generateParquetFile("nation", nation_column_names, nation_column_types, nation_schema)
generateParquetFile("region", region_column_names, region_column_types, region_schema)