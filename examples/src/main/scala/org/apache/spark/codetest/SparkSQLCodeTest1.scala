package org.apache.spark.codetest

import org.apache.spark.sql.SparkSession

object SparkSQLCodeTest1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    val df = spark.read.json("examples/src/main/resources/people.json")
    df.createOrReplaceTempView("people")
    val teenagers = spark.sql("select name from people where age>=13 and age<=19")
    teenagers.map(print(_)).collect().foreach(println)
//    teenagers.show()
//    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}
