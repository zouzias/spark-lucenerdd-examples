package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between amazon and google product's descriptions using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-products1.sh
 */
object LinkageGooglevsAmazon extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageGooglevsAmazon.getClass.getName)

    implicit val sc = SparkSession.builder.config(conf).getOrCreate()
    import sc.implicits._

    val start = System.currentTimeMillis()
    val amazonDF = sc.read.parquet("data/linkage-products1/linkage-products-amazon.parquet")
    logInfo(s"Loaded ${amazonDF.count} ACM records")
    val googleDF = sc.read.parquet("data/linkage-products1/linkage-products-google.parquet")
    logInfo(s"Loaded ${googleDF.count} DBLP records")
    val groundTruthDF = sc.read.parquet("data/linkage-products1/linkage-products-amazon-vs-google.parquet")

    val amazon = amazonDF.select("id", "title", "description", "manufacturer").map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3)))
    val googleLuceneRDD = LuceneRDD(googleDF.rdd.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3))))

    // Custom linker
    val linker: (String, String, String, String) => String = {
      case (_, name, description, manu) => {
        val nameTokens = name.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 1).distinct.mkString(" OR ")
        val descTerms = description.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 6).distinct.mkString(" OR ")
        val manuTerms = manu.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 1).mkString(" OR ")

        /*
        if (descTerms.nonEmpty && nameTokens.nonEmpty && manuTerms.nonEmpty) {
          s"(_2:(${nameTokens})) OR (_3:${descTerms}) OR (_4:${manuTerms})"
        }
        else if (nameTokens.nonEmpty && manuTerms.nonEmpty) {
          s"(_2:(${nameTokens})) OR (_4:${manuTerms})"
        }
        else if (nameTokens.nonEmpty) {
          s"_2:(${nameTokens})"
        }
        else {
          "*:*"
        }*/

        if (nameTokens.nonEmpty) {
          s"_2:(${nameTokens})"
        }
        else {
          "*:*"
        }
      }
    }

    val linkedResults = googleLuceneRDD.link(amazon.rdd, linker.tupled, 3)

    import sc.implicits._

    val linkageResults = sc.createDataFrame(linkedResults.filter(_._2.nonEmpty).map{ case (left, topDocs) => (topDocs.head.doc.textField("_1").head, left._1)})
      .toDF("idGoogleBase", "idAmazon")

    val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idAmazon").equalTo(linkageResults("idAmazon")) &&  groundTruthDF.col("idGoogleBase").equalTo(linkageResults("idGoogleBase"))).count
    val total: Double = groundTruthDF.count
    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is ${accuracy}")
    logInfo("********************************")
    // terminate spark context
    sc.stop()

  }
}

