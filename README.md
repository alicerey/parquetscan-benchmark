# parquetscan-benchmark

The repository is structured as follows:
- In the `generator` folder we store the scripts to generate parquet files from the different parquet writers. 
- In the `job` and `tpch` folders we store the scripts to run the benchmarks on each database with the respective scripts. For the DuckDB and HyPer benchmarks we use the same SQL-files. Since Spark needs an intermediate step of loading the parquet files into data frames, we hard-coded the queries into these scripts.

#### Requirements:

- Spark-Shell
- Python packages: `duckdb`, `tableauhyperapi` (requirements.txt)

## How to generate Parquet files:
### For DuckDB and HyPer:
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
### For DuckDB and HyPer:
```
./duckdb_tpch.py
```

### For Spark:
```
./spark-shell --driver-memory 12g -i tpch.scala
```

### For Umbra:
```
./umbra_tpch.py
