// in Scala
val df = spark.range(500).toDF("number")
df.select(df.col("number") + 10)

spark.range(2).toDF().collect()


import org.apache.spark.sql.types._
val b = ByteType


