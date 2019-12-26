// in Scala
val df = spark.read.format("json")
  .load("/data/flight-data/json/2015-summary.json")


df.printSchema()

spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema

import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

val myManualSchema = StructType(Array(
  StructField("DEST_COUNTRY_NAME", StringType, true),
  StructField("ORIGIN_COUNTRY_NAME", StringType, true),
  StructField("count", LongType, false,
    Metadata.fromJson("{\"hello\":\"world\"}"))
))

val df = spark.read.format("json").schema(myManualSchema)
  .load("/data/flight-data/json/2015-summary.json")

import org.apache.spark.sql.functions.{col, column}
col("someColumnName")
column("someColumnName")

$"myColumn"
'myColumn


df.col("count")


(((col("someCol") + 5) * 200) - 6) < col("otherCol")

import org.apache.spark.sql.functions.expr
expr("(((someCol + 5) * 200) - 6) < otherCol")


spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
  .columns


df.first()

import org.apache.spark.sql.Row
val myRow = Row("Hello", null, 1, false)

myRow(0) // type Any
myRow(0).asInstanceOf[String] // String
myRow.getString(0) // String
myRow.getInt(2) // Int

val df = spark.read.format("json")
  .load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}

val myManualSchema = new StructType(Array(
  new StructField("some", StringType, true),
  new StructField("col", StringType, true),
  new StructField("names", LongType, false)))
val myRows = Seq(Row("Hello", null, 1L))
val myRDD = spark.sparkContext.parallelize(myRows)
val myDf = spark.createDataFrame(myRDD, myManualSchema)
myDf.show()

val myDF = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")

df.select("DEST_COUNTRY_NAME").show(2)

df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

import org.apache.spark.sql.functions.{expr, col, column}
df.select(
    df.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME,
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME"))
  .show(2)

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))
  .show(2)

df.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

df.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")
  .show(2)

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

import org.apache.spark.sql.functions.lit
df.select(expr("*"), lit(1).as("One")).show(2)

df.withColumn("numberOne", lit(1)).show(2)

df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
  .show(2)


df.withColumn("Destination", expr("DEST_COUNTRY_NAME")).columns

df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

import org.apache.spark.sql.functions.expr

val dfWithLongColName = df.withColumn(
  "This Long Column-Name",
  expr("ORIGIN_COUNTRY_NAME"))

dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new col`")
  .show(2)


dfWithLongColName.createOrReplaceTempView("dfTableLong")

dfWithLongColName.select(col("This Long Column-Name")).columns


df.drop("ORIGIN_COUNTRY_NAME").columns


dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")


df.withColumn("count2", col("count").cast("long"))


df.filter(col("count") < 2).show(2)
df.where("count < 2").show(2)

df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
  .show(2)

df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()

df.select("ORIGIN_COUNTRY_NAME").distinct().count()


val seed = 5
val withReplacement = false
val fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

val dataFrames = df.randomSplit(Array(0.25, 0.75), seed)
dataFrames(0).count() > dataFrames(1).count() // False

import org.apache.spark.sql.Row
val schema = df.schema
val newRows = Seq(
  Row("New Country", "Other Country", 5L),
  Row("New Country 2", "Other Country 3", 1L)
)
val parallelizedRows = spark.sparkContext.parallelize(newRows)
val newDF = spark.createDataFrame(parallelizedRows, schema)
df.union(newDF)
  .where("count = 1")
  .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
  .show() // get all of them and we'll see our new rows at the end

df.sort("count").show(5)
df.orderBy("count", "DEST_COUNTRY_NAME").show(5)
df.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5)

import org.apache.spark.sql.functions.{desc, asc}
df.orderBy(expr("count desc")).show(2)
df.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(2)

spark.read.format("json").load("/data/flight-data/json/*-summary.json")
  .sortWithinPartitions("count")

df.limit(5).show()

df.orderBy(expr("count desc")).limit(6).show()

df.rdd.getNumPartitions // 1

df.repartition(5)

df.repartition(col("DEST_COUNTRY_NAME"))

df.repartition(5, col("DEST_COUNTRY_NAME"))

df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

val collectDF = df.limit(10)
collectDF.take(5) // take works with an Integer count
collectDF.show() // this prints it out nicely
collectDF.show(5, false)
collectDF.collect()


collectDF.toLocalIterator()


