package org.zouzias.spark.lucenerdd.examples.linkage.point

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD
import org.zouzias.spark.lucenerdd.spatial.point.PointLuceneRDD.PointType

/**
 * Record linkage example between countries and cities using [[PointLuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-radius.sh
 */
object PointLuceneRDDLinkageCountriesvsCapitals extends Logging {

  // 20km radius
  val Radius = 10D

  def parseDouble(s: String): Double = try { s.toDouble } catch { case _: Throwable => 0.0 }

  def WktToPoint(s: String): (Double, Double) = {
    val nums = s.dropWhile(x => x.compareTo('(') != 0).drop(1).dropRight(1)
    val coords = nums.split(" ").map(_.trim)
    (parseDouble(coords(0)), parseDouble(coords(1)))
  }

  def main(args: Array[String]): Unit = {
    // initialise spark context
    val conf = new SparkConf().setAppName(PointLuceneRDDLinkageCountriesvsCapitals.getClass.getName)

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis()

    // Load all countries
    val allCountries = spark.read.parquet("data/spatial/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    // Load all capitals
    val capitals = spark.read.parquet("data/spatial/capitals.parquet")
      .select("name", "shape")
      .rdd
      .map(row => (WktToPoint(row.getString(1)), row.getString(0)))

    def proj(point: (String, String)): PointType = {
        WktToPoint(point._1)
    }

    val points = PointLuceneRDD(capitals)
    points.cache

    val linked = points.linkByRadius(allCountries.rdd, proj, 10, 10.0, "Intersects")
    linked.cache

    linked.map(x => (x._1, x._2.headOption.map(_.doc)))
      .foreach { case (capital, country) =>
      logInfo(s"Capital of ${capital._2} is ${country})")
    }

    val end = System.currentTimeMillis()

    println("=" * 40)
    println(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    println("=" * 40)
    // terminate spark context
    spark.stop()

  }
}
