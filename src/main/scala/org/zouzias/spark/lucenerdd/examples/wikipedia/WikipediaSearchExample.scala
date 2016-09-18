package org.zouzias.spark.lucenerdd.examples.wikipedia

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

/**
 * Wikipedia search example
 */
object WikipediaSearchExample {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("WikipediaSearchExample")
    val k = 10

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val wiki = sqlContext.read.parquet("data/enwiki-latest-all-titles").select("title")
      .map(row => row.getString(0).replaceAll("_", " "))
      .map(_.replaceAll("[^a-zA-Z0-9\\s]", ""))

    val rdd = LuceneRDD(wiki.take(10000))

    val result = rdd.termQuery("_1", "argos", k)

    println(result.take(k).foreach(println))

    // terminate spark context
    sc.stop()

  }
}
