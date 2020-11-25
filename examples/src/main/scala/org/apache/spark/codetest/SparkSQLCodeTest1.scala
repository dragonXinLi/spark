package org.apache.spark.codetest

import org.apache.spark.sql.{DataFrame, SparkSession}
object SparkSQLCodeTest1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.createOrReplaceTempView("people")
    val teenagers = spark.sql("select name from people where age>=13 and age<=19")
    teenagers.show()
    spark.close()

//    val a = spark.sparkContext.parallelize(Array((1,2),(3,4)))
//    a.foreach(println)

//    a.toDF("fir","sec").createOrReplaceTempView("df")
//    val dataFrame: DataFrame = spark.sql("select * from df")
//    dataFrame.show()

//    frame.select("fir").show()
//    frame.show()
//    frame.printSchema()

  }
}
