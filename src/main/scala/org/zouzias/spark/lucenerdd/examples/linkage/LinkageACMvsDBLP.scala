package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between ACM and DBLP using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-acm.sh
 */
object LinkageACMvsDBLP extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageACMvsDBLP.getClass.getName)

    implicit val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis()

    val acmDF = spark.read.parquet("data/linkage-papers2/linkage-papers-acm.parquet")
    logInfo(s"Loaded ${acmDF.count} ACM records")
    val dblp2DF = spark.read.parquet("data/linkage-papers2/linkage-papers-dblp2.parquet")
    logInfo(s"Loaded ${acmDF.count} DBLP records")
    val groundTruthDF = spark.read.parquet("data/linkage-papers2/linkage-papers-acm-vs-dblp2.parquet")

    val dblp2 = LuceneRDD(dblp2DF)
    dblp2.cache()

    // Link is the author tokens or title tokens match. Combine all tokens by an OR clause
    val linker: Row => String = {
      case row => {
        val title = row.getString(row.fieldIndex("title"))
        val authors = row.getString(row.fieldIndex("authors"))

        val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
        val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

        if (authorsTerms.nonEmpty) {
          s"(title:(${titleTokens})) OR (author:${authorsTerms})"
        }
        else{
          s"title:(${titleTokens})"
        }
      }
    }

    val linkedResults = dblp2.linkDataFrame(acmDF, linker, 10)

    val linkageResults = spark.createDataFrame(linkedResults.filter(_._2.nonEmpty).map{ case (acm, topDocs) => (topDocs.head.doc.textField("id").head, acm.getInt(acm.fieldIndex("id")).toString)})
      .toDF("idDBLP", "idACM")

    val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idACM").equalTo(linkageResults("idACM"))).count
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
    spark.stop()

  }
}

