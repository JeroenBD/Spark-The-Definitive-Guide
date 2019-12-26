// in Scala
val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
  .toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")
val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
  .toDF("id", "status")


person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

val wrongJoinExpression = person.col("name") === graduateProgram.col("school")


person.join(graduateProgram, joinExpression).show()

var joinType = "inner"


person.join(graduateProgram, joinExpression, joinType).show()


joinType = "outer"


person.join(graduateProgram, joinExpression, joinType).show()


joinType = "left_outer"


graduateProgram.join(person, joinExpression, joinType).show()


joinType = "right_outer"


person.join(graduateProgram, joinExpression, joinType).show()


joinType = "left_semi"


graduateProgram.join(person, joinExpression, joinType).show()

val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())

gradProgram2.createOrReplaceTempView("gradProgram2")


gradProgram2.join(person, joinExpression, joinType).show()


joinType = "left_anti"
graduateProgram.join(person, joinExpression, joinType).show()


joinType = "cross"
graduateProgram.join(person, joinExpression, joinType).show()


person.crossJoin(graduateProgram).show()


import org.apache.spark.sql.functions.expr

person.withColumnRenamed("id", "personId")
  .join(sparkStatus, expr("array_contains(spark_status, id)")).show()


val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")


val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
  "graduate_program")


person.join(gradProgramDupe, joinExpr).show()


person.join(gradProgramDupe, joinExpr).select("graduate_program").show()


person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()


person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
  .select("graduate_program").show()


val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()


val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
val joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
person.join(gradProgram3, joinExpr).show()


val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

person.join(graduateProgram, joinExpr).explain()


import org.apache.spark.sql.functions.broadcast
val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
person.join(broadcast(graduateProgram), joinExpr).explain()


