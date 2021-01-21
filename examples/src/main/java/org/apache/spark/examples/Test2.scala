package org.apache.spark.examples

object Test2 {
  def main(args: Array[String]): Unit = {
    implicit def a(d:Double)=d.toInt
    val i1:Int=3.5
    println(i1)
  }
}
