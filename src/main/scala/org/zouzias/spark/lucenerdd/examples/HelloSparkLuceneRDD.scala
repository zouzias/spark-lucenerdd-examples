package org.zouzias.spark.lucenerdd.examples

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Minimalistic example for [[LuceneRDD]]
 */
object HelloSparkLuceneRDD extends Logging {
  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(HelloSparkLuceneRDD.getClass.getName)
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    implicit val sc: SparkContext = spark.sparkContext

    val array = Array("Hello", "world")
    val rdd = LuceneRDD(array)
    val count = rdd.count

    logInfo("=" * 30)
    logInfo(s"| LuceneRDD results count is $count")
    logInfo("=" * 30)

    // terminate spark context
    sc.stop()
  }
}
