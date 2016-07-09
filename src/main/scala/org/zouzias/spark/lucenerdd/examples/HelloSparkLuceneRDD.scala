package org.zouzias.spark.lucenerdd.examples

// import required spark classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

// define main method (scala entry point)
object HelloSparkLuceneRDD {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("HelloSparkLuceneRDD")

    //
    implicit val sc = new SparkContext(conf)

    val array = Array("Hello", "world")
    val rdd = LuceneRDD(array)
    val count = rdd.count

    println("=" * 10)
    println(s"Count is ${count}")
    println("=" * 10)

    // terminate spark context
    sc.stop()

  }
}
