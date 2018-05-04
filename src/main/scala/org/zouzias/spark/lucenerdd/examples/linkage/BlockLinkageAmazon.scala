package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between amazon and itself blocked by manufacturer using
  * [[LuceneRDD.blockEntityLinkage]] method
 *
 * You can run this locally with, ./spark-blocklinkage-amazon.sh
 */
object BlockLinkageAmazon extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageGooglevsAmazon.getClass.getName)

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()

    val start = System.currentTimeMillis()
    val amazonDF = spark.read.parquet("data/linkage-products1/linkage-products-amazon.parquet")
    logInfo(s"Loaded ${amazonDF.count} ACM records")

    val amazon = amazonDF.select("id", "title", "description", "manufacturer")

    // Custom linker
    val linker: Row => String = {
      case row => {
        val name = row.getString(row.fieldIndex("title"))
        val description = row.getString(row.fieldIndex("description"))
        val manu = row.getString(row.fieldIndex("manufacturer"))
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
          s"title:(${nameTokens})"
        }
        else {
          "*:*"
        }
      }
    }

    val linkedResults = LuceneRDD.blockEntityLinkage(amazonDF, amazonDF,
      linker, /* Link by title similarity */
      Array("manufacturer"),
      Array("manufacturer")
    )

    val linkageResults = spark.createDataFrame(linkedResults
      .filter(_._2.nonEmpty)
      .map{ case (left, topDocs) =>
        (topDocs.head.doc.textField("id").headOption,
          left.getString(left.fieldIndex("id"))
        )
      })
      .toDF("id", "id_amazon")


    linkageResults.show()


    val correctHits: Double = linkageResults.count
    logInfo(s"Correct hits are ${correctHits}")
    val total: Double = amazonDF.count
    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is ${accuracy}")
    logInfo("********************************")
    // terminate spark context
    spark.stop()

  }
}

