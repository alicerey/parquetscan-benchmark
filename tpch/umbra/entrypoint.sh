#!/bin/bash

cd /umbra
cp -r /scripts/* /umbra
echo "preparation steps"
umbra-sql --createdb umbra-db nooutput.sql scanall.sql
echo "benchmarks"
umbra-sql umbra-db repeat.sql nooutput.sql tpch_all_parquet
