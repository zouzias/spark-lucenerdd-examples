package org.zouzias.spark.lucenerdd.examples.linkage.shape

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between countries and cities using [[ShapeLuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-radius.sh
 */
object ShapeLuceneRDDLinkageCountriesvsCapitals extends Logging {

  // 20km radius
  val Radius = 10D

  def main(args: Array[String]): Unit = {
    // initialise spark context
    val conf = new SparkConf().setAppName(ShapeLuceneRDDLinkageCountriesvsCapitals.getClass.getName)

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis()

    // Load all countries
    val allCountries = spark.read.parquet("data/spatial/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    // Load all capitals
    val capitals = spark.read.parquet("data/spatial/capitals.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    def parseDouble(s: String): Double = try { s.toDouble } catch { case _: Throwable => 0.0 }

    def coords(point: (String, String)): (Double, Double) = {
      val str = point._1
      val nums = str.dropWhile(x => x.compareTo('(') != 0).drop(1).dropRight(1)
      val coords = nums.split(" ").map(_.trim)
      (parseDouble(coords(0)), parseDouble(coords(1)))
    }

    val shapes = ShapeLuceneRDD(allCountries)
    shapes.cache

    val linked = shapes.linkByRadius(capitals.rdd, coords, Radius)
    linked.cache

    linked.map(x => (x._1, x._2.headOption.flatMap(_.doc.textField("_1")))).foreach { case (capital, country) =>
      logInfo(s"Capital of ${country.getOrElse("Unknown")} is ${capital._2} (${capital._1})")
    }

    val end = System.currentTimeMillis()

    println("=" * 40)
    println(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    println("=" * 40)
    // terminate spark context
    spark.stop()

  }
}
