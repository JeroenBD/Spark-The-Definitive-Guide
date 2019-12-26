spark.sparkContext
: converts a Dataset[Long] to RDD[Long]
spark.range(500).rdd

spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0))

spark.range(10).rdd.toDF()

val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
  .split(" ")
val words = spark.sparkContext.parallelize(myCollection, 2)

words.setName("myWords")
words.name // myWords


spark.sparkContext.textFile("/some/path/withTextFiles")


spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")


words.distinct().count()

def startsWithS(individual:String) = {
  individual.startsWith("S")
}

words.filter(word => startsWithS(word)).collect()

val words2 = words.map(word => (word, word(0), word.startsWith("S")))

words2.filter(record => record._3).take(5)

words.flatMap(word => word.toSeq).take(5)

words.sortBy(word => word.length() * -1).take(2)

val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))

spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210

def wordLengthReducer(leftWord:String, rightWord:String): String = {
  if (leftWord.length > rightWord.length)
    return leftWord
  else
    return rightWord
}

words.reduce(wordLengthReducer)


words.count()


val confidence = 0.95
val timeoutMilliseconds = 400
words.countApprox(timeoutMilliseconds, confidence)


words.countApproxDistinct(0.05)


words.countApproxDistinct(4, 10)


words.countByValue()


words.countByValueApprox(1000, 0.95)


words.first()


spark.sparkContext.parallelize(1 to 20).max()
spark.sparkContext.parallelize(1 to 20).min()


words.take(5)
words.takeOrdered(5)
words.top(5)
val withReplacement = true
val numberToTake = 6
val randomSeed = 100L
words.takeSample(withReplacement, numberToTake, randomSeed)


words.saveAsTextFile("file:/tmp/bookTitle")

import org.apache.hadoop.io.compress.BZip2Codec
words.saveAsTextFile("file:/tmp/bookTitleCompressed", classOf[BZip2Codec])


words.saveAsObjectFile("/tmp/my/sequenceFilePath")


words.cache()

words.getStorageLevel


spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
words.checkpoint()


words.pipe("wc -l").collect()

words.mapPartitions(part => Iterator[Int](1)).sum() // 2

def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
  withinPartIterator.toList.map(
    value => s"Partition: $partitionIndex => $value").iterator
}
words.mapPartitionsWithIndex(indexedFunc).collect()


words.foreachPartition { iter =>
  import java.io._
  import scala.util.Random
  val randomFileName = new Random().nextInt()
  val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
  while (iter.hasNext) {
      pw.write(iter.next())
  }
  pw.close()
}

spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()
// Array(Array(Hello), Array(World))


