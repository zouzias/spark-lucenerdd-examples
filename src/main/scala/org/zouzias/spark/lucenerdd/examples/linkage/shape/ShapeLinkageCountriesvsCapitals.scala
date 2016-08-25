package org.zouzias.spark.lucenerdd.examples.linkage.shape

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.spatial.shape.ShapeLuceneRDD
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd._

/**
 * Record linkage example between countries and cities using [[ShapeLuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-radius.sh
 */
object ShapeLinkageCountriesvsCapitals {

  // 50km radius
  val Radius = 20D

  def main(args: Array[String]): Unit = {
    // initialise spark context
    val conf = new SparkConf().setAppName(ShapeLinkageCountriesvsCapitals.getClass.getName)

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load all countries
    val allCountries = sqlContext.read.parquet("data/spatial/countries-poly.parquet")
      .select("name", "shape")
      .map(row => (row.getString(1), row.getString(0)))

    // Load all capitals
    val capitals = sqlContext.read.parquet("data/spatial/capitals.parquet")
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

    val linked = shapes.linkByRadius(capitals, coords, Radius)
    linked.cache

    linked.map(x => (x._1, x._2.map(_.doc.textField("_1")))).foreach(println)

    // terminate spark context
    sc.stop()

  }
}
