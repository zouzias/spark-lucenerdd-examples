package org.zouzias.spark.lucenerdd.examples.wikipedia

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

/**
 * Capitals search example
 */
object CapitalsSearchExample {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaSearchExample")
    val k = 10

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val capitals = sqlContext.read.parquet("data/spatial/capitals.parquet").select("name", "country")

    val rdd = LuceneRDD(capitals)

    val result = rdd.termQuery("name", "ottawa", k)

    println(result.take(k).foreach(println))

    // terminate spark context
    sc.stop()

  }
}
