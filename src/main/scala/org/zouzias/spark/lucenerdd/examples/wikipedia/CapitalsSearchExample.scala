package org.zouzias.spark.lucenerdd.examples.wikipedia

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Capitals search example
 */
object CapitalsSearchExample extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName("CapitalsSearchExample")
    val k = 10

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()

    val start = System.currentTimeMillis()

    val capitals = spark.read.parquet("data/spatial/capitals.parquet").select("name", "country")

    val luceneRDD = LuceneRDD(capitals)

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
