val args = sc.getConf.get("spark.driver.args").split("\\s+")
val compressed = args contains "compressed"
val onefile = args contains "onefile"

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame}

// config for compression or no compression
val newConf = if (compressed) spark.sparkContext.getConf.set("spark.sql.parquet.binaryAsString","true") else spark.sparkContext.getConf.set("spark.sql.parquet.compression.codec","none").set("spark.sql.parquet.binaryAsString","true")

// use newConf
spark.sparkContext.stop()
val spark = SparkSession.builder.config(newConf).getOrCreate()
val sc = spark.sparkContext

/// load tpch data from *.tbl files
val DATA_PATH = args(0)
val STORAGE_PATH = args(1)

/// force not nullable
def setNullableStateForAllColumns( df: DataFrame, nullable: Boolean) : DataFrame = {
  // get schema
  val schema = df.schema
  // modify [[StructField] with name `cn`
  val newSchema = StructType(schema.map {
    case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = nullable, m)
  })
  // apply new schema
  df.sqlContext.createDataFrame( df.rdd, newSchema )
}

def getCustomer(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("c_custkey", IntegerType, false),
      StructField("c_name", StringType, false),
      StructField("c_address", StringType, false),
      StructField("c_nationkey", IntegerType, false),
      StructField("c_phone", StringType, false),
      StructField("c_acctbal", DecimalType(12, 2), false),
      StructField("c_mktsegment", StringType, false),
      StructField("c_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/customer.tbl"), false)
}

def getLineitem(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("l_orderkey", IntegerType, false),
      StructField("l_partkey", IntegerType, false),
      StructField("l_suppkey", IntegerType, false),
      StructField("l_linenumber", IntegerType, false),
      StructField("l_quantity", DecimalType(12, 2), false),
      StructField("l_extendedprice", DecimalType(12, 2), false),
      StructField("l_discount", DecimalType(12, 2), false),
      StructField("l_tax", DecimalType(12, 2), false),
      StructField("l_returnflag", StringType, false),
      StructField("l_linestatus", StringType, false),
      StructField("l_shipdate", DateType, false),
      StructField("l_commitdate", DateType, false),
      StructField("l_receiptdate", DateType, false),
      StructField("l_shipinstruct", StringType, false),
      StructField("l_shipmode", StringType, false),
      StructField("l_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/lineitem.tbl"), false)
}

def getNation(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("n_nationkey", IntegerType, false),
      StructField("n_name", StringType, false),
      StructField("n_regionkey", IntegerType, false),
      StructField("n_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/nation.tbl"), false)
}

def getOrder(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("o_orderkey", IntegerType, false),
      StructField("o_custkey", IntegerType, false),
      StructField("o_orderstatus", StringType, false),
      StructField("o_totalprice", DecimalType(12, 2), false),
      StructField("o_orderdate", DateType, false),
      StructField("o_orderpriority", StringType, false),
      StructField("o_clerk", StringType, false),
      StructField("o_shippriority", IntegerType, false),
      StructField("o_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/orders.tbl"), false)
}

def getPart(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("p_partkey", IntegerType, false),
      StructField("p_name", StringType, false),
      StructField("p_mfgr", StringType, false),
      StructField("p_brand", StringType, false),
      StructField("p_type", StringType, false),
      StructField("p_size", IntegerType, false),
      StructField("p_container", StringType, false),
      StructField("p_retailprice", DecimalType(12, 2), false),
      StructField("p_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/part.tbl"), false)
}

def getPartsupp(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("ps_partkey", IntegerType, false),
      StructField("ps_suppkey", IntegerType, false),
      StructField("ps_availqty", IntegerType, false),
      StructField("ps_supplycost", DecimalType(12, 2), false),
      StructField("ps_commment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/partsupp.tbl"), false)
}

def getRegion(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("r_regionkey", IntegerType, false),
      StructField("r_name", StringType, false),
      StructField("r_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/region.tbl"), false)
}

def getSupplier(): DataFrame = {
  setNullableStateForAllColumns(spark.read.format("csv").schema(StructType(
    List(
      StructField("s_suppkey", IntegerType, false),
      StructField("s_name", StringType, false),
      StructField("s_address", StringType, false),
      StructField("s_nationkey", IntegerType, false),
      StructField("s_phone", StringType, false),
      StructField("s_acctbal", DecimalType(12, 2), false),
      StructField("s_comment", StringType, false)
    )
  )).option("delimiter", "|").load(DATA_PATH + "/supplier.tbl"), false)
}

if (onefile) {
  getRegion.repartition(1).write.parquet(STORAGE_PATH + "/region")
  getNation.repartition(1).write.parquet(STORAGE_PATH + "/nation")
  getCustomer.repartition(1).write.parquet(STORAGE_PATH + "/customer")
  getLineitem.repartition(1).write.parquet(STORAGE_PATH + "/lineitem")
  getOrder.repartition(1).write.parquet(STORAGE_PATH + "/orders")
  getPart.repartition(1).write.parquet(STORAGE_PATH + "/part")
  getPartsupp.repartition(1).write.parquet(STORAGE_PATH + "/partsupp")
  getSupplier.repartition(1).write.parquet(STORAGE_PATH + "/supplier")
} else {
  getRegion.write.parquet(STORAGE_PATH + "/region")
  getNation.write.parquet(STORAGE_PATH + "/nation")
  getCustomer.write.parquet(STORAGE_PATH + "/customer")
  getLineitem.write.parquet(STORAGE_PATH + "/lineitem")
  getOrder.write.parquet(STORAGE_PATH + "/orders")
  getPart.write.parquet(STORAGE_PATH + "/part")
  getPartsupp.write.parquet(STORAGE_PATH + "/partsupp")
  getSupplier.write.parquet(STORAGE_PATH + "/supplier")
}
