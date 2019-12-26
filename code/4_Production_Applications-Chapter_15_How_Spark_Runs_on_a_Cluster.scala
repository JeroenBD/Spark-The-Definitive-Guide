// Creating a SparkSession in Scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Databricks Spark Example")
  .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
  .getOrCreate()

import org.apache.spark.SparkContext
val sc = SparkContext.getOrCreate()


step4.explain()


spark.conf.set("spark.sql.shuffle.partitions", 50)


