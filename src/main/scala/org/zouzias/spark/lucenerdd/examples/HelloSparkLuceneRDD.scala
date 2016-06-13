package org.zouzias.spark.lucenerdd.examples

// import required spark classes
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

// define main method (scala entry point)
object HelloSparkLuceneRDD {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("HelloSparkLuceneRDD")
    val sc = new SparkContext(conf)

    val array = Array("Hello", "world")
    val rdd = LuceneRDD(array)
    rdd.count

    // terminate spark context
    sc.stop()

  }
}
