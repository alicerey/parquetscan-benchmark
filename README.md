# parquetscan-benchmark

The repository is structured as follows:
- In the `generator` folder we store the scripts to generate parquet files from the different parquet writers. 
- In the `job` and `tpch` folders we store the scripts to run the benchmarks on each database.

#### Requirements:

- Spark-Shell
- Python packages: `duckdb`, `tableauhyperapi` (use `requirements.txt` to get used versions)

## How to generate Parquet files:
### For DuckDB and Arrow:
```
./tpch-data-generator.py <source-path> <destination-path>
```

### For Spark:
```
./spark-shell --conf spark.driver.args="<source-path> <destination-path> [compressed] [onefile]"
```
> :load tpch-data-generator.scala

## How to run the Benchmarks:
The benchmark scripts assume that the parquet files are located in the current directory.
### For DuckDB and Hyper:
```
./duckdb_tpch.py
```

### For Spark:
```
./spark-shell --driver-memory 12g -i tpch.scala
```

### For Umbra:
```
./umbra_tpch.sh
