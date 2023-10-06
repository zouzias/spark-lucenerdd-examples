package org.zouzias.spark.lucenerdd.examples.wikipedia

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.apache.spark.internal.Logging

/**
 * Example that demonstrates how to search on a list of capital names using [[LuceneRDD]]
 *
 * Search over all capitals for a specific capital
 */
object CapitalsSearchExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("CapitalsSearchExample")
    val k = 10

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()

    val start = System.currentTimeMillis()

    // Load DataFrame and instantiate LuceneRDD
    val capitals = spark.read.parquet("data/spatial/capitals.parquet").select("name", "country")
    val luceneRDD = LuceneRDD(capitals)

    // Perform a term query
    val result = luceneRDD.termQuery("name", "ottawa", k)
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo(result.take(k).mkString("\n"))

    // terminate spark context
    spark.stop()
  }
}
