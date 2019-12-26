// in Scala
val static = spark.read.json("/data/activity-data/")
val dataSchema = static.schema

val streaming = spark.readStream.schema(dataSchema)
  .option("maxFilesPerTrigger", 1).json("/data/activity-data")

val activityCounts = streaming.groupBy("gt").count()


spark.conf.set("spark.sql.shuffle.partitions", 5)

val activityQuery = activityCounts.writeStream.queryName("activity_counts")
  .format("memory").outputMode("complete")
  .start()


activityQuery.awaitTermination()


spark.streams.active

for( i <- 1 to 5 ) {
    spark.sql("SELECT * FROM activity_counts").show()
    Thread.sleep(1000)
}

import org.apache.spark.sql.functions.expr
val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
  .where("stairs")
  .where("gt is not null")
  .select("gt", "model", "arrival_time", "creation_time")
  .writeStream
  .queryName("simple_transform")
  .format("memory")
  .outputMode("append")
  .start()

val deviceModelStats = streaming.cube("gt", "model").avg()
  .drop("avg(Arrival_time)")
  .drop("avg(Creation_Time)")
  .drop("avg(Index)")
  .writeStream.queryName("device_counts").format("memory").outputMode("complete")
  .start()

val historicalAgg = static.groupBy("gt", "model").avg()
val deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")
  .cube("gt", "model").avg()
  .join(historicalAgg, Seq("gt", "model"))
  .writeStream.queryName("device_counts").format("memory").outputMode("complete")
  .start()

// Subscribe to 1 topic
val ds1 = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()
// Subscribe to multiple topics
val ds2 = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1,topic2")
  .load()
// Subscribe to a pattern of topics
val ds3 = spark.readStream.format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribePattern", "topic.*")
  .load()

ds1.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream.format("kafka")
  .option("checkpointLocation", "/to/HDFS-compatible/dir")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .start()
ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  .writeStream.format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("checkpointLocation", "/to/HDFS-compatible/dir")
  .option("topic", "topic1")
  .start()


//in Scala
datasetOfString.write.foreach(new ForeachWriter[String] {
  def open(partitionId: Long, version: Long): Boolean = {
    // open a database connection
  }
  def process(record: String) = {
    // write string to connection
  }
  def close(errorOrNull: Throwable): Unit = {
    // close the connection
  }
})

val socketDF = spark.readStream.format("socket")
  .option("host", "localhost").option("port", 9999).load()


activityCounts.format("console").write()

activityCounts.writeStream.format("memory").queryName("my_device_table")

import org.apache.spark.sql.streaming.Trigger

activityCounts.writeStream.trigger(Trigger.ProcessingTime("100 seconds"))
  .format("console").outputMode("complete").start()

import org.apache.spark.sql.streaming.Trigger

activityCounts.writeStream.trigger(Trigger.Once())
  .format("console").outputMode("complete").start()

case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String,
  count: BigInt)
val dataSchema = spark.read
  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
  .schema
val flightsDF = spark.readStream.schema(dataSchema)
  .parquet("/data/flight-data/parquet/2010-summary.parquet/")
val flights = flightsDF.as[Flight]
def originIsDestination(flight_row: Flight): Boolean = {
  return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
}
flights.filter(flight_row => originIsDestination(flight_row))
  .groupByKey(x => x.DEST_COUNTRY_NAME).count()
  .writeStream.queryName("device_counts").format("memory").outputMode("complete")
  .start()


