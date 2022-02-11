val args = sc.getConf.get("spark.driver.args").split("\\s+")
val compressed = args contains "compressed"
val onefile = args contains "onefile"

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.SparkSession

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
def setNullableStateForColumns( df: DataFrame, nullable: Boolean, columns: List[String]) : DataFrame = {
  // get schema
  val schema = df.schema
  // modify [[StructField] with name `cn`
  val newSchema = StructType(schema.map {
    case StructField( c, t, _, m) ⇒ StructField( c, t, nullable = (if(columns contains c)  nullable else !nullable), m)
  })
  // apply new schema
  df.sqlContext.createDataFrame( df.rdd, newSchema )
}

def getAkaName(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("imdb_index", StringType, true),
      StructField("name_pcode_cf", StringType, true),
      StructField("name_pcode_nf", StringType, true),
      StructField("surname_pcode", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/aka_name.csv"), false, List("id", "person_id", "name"))
}


def getAkaTitle(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("title", StringType, false),
      StructField("imdb_index", StringType, true),
      StructField("kind_id", IntegerType, false),
      StructField("production_year", IntegerType, true),
      StructField("phonetic_code", StringType, true),
      StructField("episode_of_id", IntegerType, true),
      StructField("season_nr", IntegerType, true),
      StructField("episode_nr", IntegerType, true),
      StructField("note", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/aka_title.csv"), false, List("id", "movie_id", "title", "kind_id"))
}

def getCastInfo(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("person_role_id", IntegerType, true),
      StructField("note", StringType, true),
      StructField("nr_order", IntegerType, true),
      StructField("role_id", IntegerType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/cast_info.csv"), false, List("id", "person_id", "movie_id", "role_id"))
}

def getCharName(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("imdb_index", StringType, true),
      StructField("imdb_id", IntegerType, true),
      StructField("name_pcode_nf", StringType, true),
      StructField("surname_pcode", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/char_name.csv"), false, List("id", "name"))
}

def getCompCastType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/comp_cast_type.csv"), false, List("id", "kind"))
}

def getCompanyName(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("country_code", StringType, true),
      StructField("imdb_id", IntegerType, true),
      StructField("name_pcode_nf", StringType, true),
      StructField("name_pcode_sf", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/company_name.csv"), false, List("id", "name"))
}

def getCompanyType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/company_type.csv"), false, List("id", "kind"))
}

def getCompleteCast(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, true),
      StructField("subject_id", IntegerType, false),
      StructField("status_id", IntegerType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/complete_cast.csv"), false, List("id", "subject", "status_id"))
}

def getInfoType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("info", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/info_type.csv"), false, List("id", "info"))
}

def getKeyword(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("keyword", StringType, false),
      StructField("phonetic_code", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/keyword.csv"), false, List("id", "keyword"))
}

def getKindType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("kind", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/kind_type.csv"), false, List("id", "kind"))
}

def getLinkType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("link", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/link_type.csv"), false, List("id", "link"))
}

def getMovieCompanies(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("company_id", IntegerType, false),
      StructField("company_type_id", IntegerType, false),
      StructField("note", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/movie_companies.csv"), false, List("id", "movie_id", "company_id", "company_type_id"))
}

def getMovieInfo(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      StructField("info", StringType, false),
      StructField("note", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/movie_info.csv"), false, List("id", "movie_id", "info_type_id", "info"))
}

def getMovieInfoIdx(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      StructField("info", StringType, false),
      StructField("note", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/movie_info_idx.csv"), false, List("id", "movie_id", "info_type_id", "info"))
}

def getMovieKeyword(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("keyword_id", IntegerType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/movie_keyword.csv"), false, List("id", "movie_id", "keyword_id"))
}

def getMovieLink(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("movie_id", IntegerType, false),
      StructField("linked_movie_id", IntegerType, false),
      StructField("link_type_id", IntegerType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/movie_link.csv"), false, List("id", "movie_id", "linked_movie_id", "link_type_id"))
}

def getName(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("imdb_index", StringType, true),
      StructField("imdb_id", IntegerType, true),
      StructField("gender", StringType, true),
      StructField("name_pcode_cf", StringType, true),
      StructField("name_pcode_nf", StringType, true),
      StructField("surname_pcode", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/name.csv"), false, List("id", "name"))
}

def getPersonInfo(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("person_id", IntegerType, false),
      StructField("info_type_id", IntegerType, false),
      StructField("info", StringType, false),
      StructField("note", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/person_info.csv"), false, List("id", "person_id", "info_type_id", "info"))
}

def getRoleType(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("role", StringType, false)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/role_type.csv"), false, List("id", "role"))
}

def getTitle(): DataFrame = {
  setNullableStateForColumns(spark.read.format("csv").schema(StructType(
    Seq(
      StructField("id", IntegerType, false),
      StructField("title", StringType, false),
      StructField("imdb_index", StringType, true),
      StructField("kind_id", IntegerType, false),
      StructField("production_year", IntegerType, true),
      StructField("imdb_id", IntegerType, true),
      StructField("phonetic_code", StringType, true),
      StructField("episode_of_id", IntegerType, true),
      StructField("season_nr", IntegerType, true),
      StructField("episode_nr", IntegerType, true),
      StructField("series_years", StringType, true),
      StructField("md5sum", StringType, true)
    )
  )).option("delimiter", ",").load(DATA_PATH + "/title.csv"), false, List("id", "title", "kind_id"))
}


if (onefile) {
  getAkaName().repartition(1).write.parquet(STORAGE_PATH + "/aka_name")
  getAkaTitle().repartition(1).write.parquet(STORAGE_PATH + "/aka_title")
  getCastInfo().repartition(1).write.parquet(STORAGE_PATH + "/cast_info")
  getCharName().repartition(1).write.parquet(STORAGE_PATH + "/char_name")
  getCompCastType().repartition(1).write.parquet(STORAGE_PATH + "/comp_cast_type")
  getCompanyName().repartition(1).write.parquet(STORAGE_PATH + "/company_name")
  getCompanyType().repartition(1).write.parquet(STORAGE_PATH + "/company_type")
  getCompleteCast().repartition(1).write.parquet(STORAGE_PATH + "/complete_cast")
  getInfoType().repartition(1).write.parquet(STORAGE_PATH + "/info_type")
  getKeyword().repartition(1).write.parquet(STORAGE_PATH + "/keyword")
  getKindType().repartition(1).write.parquet(STORAGE_PATH + "/kind_type")
  getLinkType().repartition(1).write.parquet(STORAGE_PATH + "/link_type")
  getMovieCompanies().repartition(1).write.parquet(STORAGE_PATH + "/movie_companies")
  getMovieInfo().repartition(1).write.parquet(STORAGE_PATH + "/movie_info")
  getMovieInfoIdx().repartition(1).write.parquet(STORAGE_PATH + "/movie_info_idx")
  getMovieKeyword().repartition(1).write.parquet(STORAGE_PATH + "/movie_keyword")
  getMovieLink().repartition(1).write.parquet(STORAGE_PATH + "/movie_link")
  getName().repartition(1).write.parquet(STORAGE_PATH + "/name")
  getPersonInfo().repartition(1).write.parquet(STORAGE_PATH + "/person_info")
  getRoleType().repartition(1).write.parquet(STORAGE_PATH + "/role_type")
  getTitle().repartition(1).write.parquet(STORAGE_PATH + "/title")
} else {
  getAkaName().write.parquet(STORAGE_PATH + "/aka_name")
  getAkaTitle().write.parquet(STORAGE_PATH + "/aka_title")
  getCastInfo().write.parquet(STORAGE_PATH + "/cast_info")
  getCharName().write.parquet(STORAGE_PATH + "/char_name")
  getCompCastType().write.parquet(STORAGE_PATH + "/comp_cast_type")
  getCompanyName().write.parquet(STORAGE_PATH + "/company_name")
  getCompanyType().write.parquet(STORAGE_PATH + "/company_type")
  getCompleteCast().write.parquet(STORAGE_PATH + "/complete_cast")
  getInfoType().write.parquet(STORAGE_PATH + "/info_type")
  getKeyword().write.parquet(STORAGE_PATH + "/keyword")
  getKindType().write.parquet(STORAGE_PATH + "/kind_type")
  getLinkType().write.parquet(STORAGE_PATH + "/link_type")
  getMovieCompanies().write.parquet(STORAGE_PATH + "/movie_companies")
  getMovieInfo().write.parquet(STORAGE_PATH + "/movie_info")
  getMovieInfoIdx().write.parquet(STORAGE_PATH + "/movie_info_idx")
  getMovieKeyword().write.parquet(STORAGE_PATH + "/movie_keyword")
  getMovieLink().write.parquet(STORAGE_PATH + "/movie_link")
  getName().write.parquet(STORAGE_PATH + "/name")
  getPersonInfo().write.parquet(STORAGE_PATH + "/person_info")
  getRoleType().write.parquet(STORAGE_PATH + "/role_type")
  getTitle().write.parquet(STORAGE_PATH + "/title")
}
