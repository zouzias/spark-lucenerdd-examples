package org.zouzias.spark.lucenerdd.examples.wikipedia

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

/**
 * Capitals search example
 */
object CapitalsSearchExample {

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

    println("=" * 40)
    println(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    println("=" * 40)

    println(result.take(k).foreach(println))

    // terminate spark context
    spark.stop()

  }
}
