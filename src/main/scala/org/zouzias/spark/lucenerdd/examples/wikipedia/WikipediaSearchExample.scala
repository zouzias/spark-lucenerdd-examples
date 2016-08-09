package org.zouzias.spark.lucenerdd.examples.wikipedia

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

    //
    implicit val sc = new SparkContext(conf)

    val wiki = sc.textFile("/Users/taazoan3/recordLinkageData/wikipedia/enwiki-latest-all-titles").map(_.replaceAll("_", " ")).map(_.replaceAll("[^a-zA-Z0-9\\s]", ""))
    val rdd = LuceneRDD(wiki.take(10000))


    val result = rdd.termQuery("_1", "argos", 10)

    println(result.seq.size)

    // terminate spark context
    sc.stop()

  }
}
