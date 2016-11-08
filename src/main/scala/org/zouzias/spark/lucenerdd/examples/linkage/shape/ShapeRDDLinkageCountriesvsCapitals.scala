package org.zouzias.spark.lucenerdd.examples.linkage.shape

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.spatial.shape._
import org.zouzias.spark.lucenerdd.spatial.shape.rdds.ShapeRDD

/**
 * Record linkage example between countries and cities using [[ShapeRDD]]
 *
 * You can run this locally with, ./spark-linkage-radius.sh
 */
object ShapeRDDLinkageCountriesvsCapitals {

  // 20km radius
  val Radius = 20D

  def main(args: Array[String]): Unit = {
    // initialise spark context
    val conf = new SparkConf().setAppName(ShapeRDDLinkageCountriesvsCapitals.getClass.getName)

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

    val shapes = ShapeRDD(allCountries)
    shapes.cache

    val linked = shapes.linkByRadius(capitals.rdd, coords, Radius)
    linked.cache

    println("=" * 40)
    println(s"Total is ${linked.count()}")
    println("=" * 40)


    shapes.postLinker(linked).map(x => (x._1._2, x._2._2)).foreach(println)

    val end = System.currentTimeMillis()

    println("=" * 40)
    println(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    println("=" * 40)
    // terminate spark context
    spark.stop()

  }
}
