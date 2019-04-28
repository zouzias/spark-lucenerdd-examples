package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.logging.Logging
import org.apache.spark.sql

/**
 * Record linkage example between ACM and DBLP using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-acm.sh
 */
object LinkageACMvsDBLP extends Logging {

  def tokenizer(s: String, threshold: Int, comb: String = "OR"): String = {
    s.split(" ")
      .flatMap(_.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
      .filter(_.length > threshold).mkString(s" $comb ")
  }

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageACMvsDBLP.getClass.getName)
    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
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
    // Define a  custom linker (defines your linkage logic)
    val linker: Row => String = {row => {

      // Get title and authors fields
      val title = row.getString(row.fieldIndex("title"))
      val authors = row.getString(row.fieldIndex("authors"))

      val titleTokens = tokenizer(title, 3) // Replace 8 with 3
      val authorsTerms = tokenizer(authors, 3) // Use 3 to get 0.97 accuracy

      if (titleTokens.nonEmpty && authorsTerms.nonEmpty) {
        s"(title:(${titleTokens})) OR (authors:${authorsTerms})"
      }
      else if (titleTokens.nonEmpty){
        s"title:(${titleTokens})"
      }
      else if ( authorsTerms.nonEmpty) {
        s"(authors:${authorsTerms})"
      }
      else {
        "*:*"
      }
    }
    }

    // Perform linkage and return top-5 results
    val linkedResults = dblp2.linkDataFrame(acmDF, linker, 3).filter(_._2.nonEmpty)

    // Compute the performance of linkage (accuracy)
    val linkageResults = spark.createDataFrame(linkedResults.map{ case (acm, topDocs) =>
      (topDocs.head.getString(topDocs.head.fieldIndex("id")),
        acm.getInt(acm.fieldIndex("id")).toString)})
      .toDF("idDBLP", "idACM")

    val correctHits: Double = linkageResults
      .join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP"))
        && groundTruthDF.col("idACM").equalTo(linkageResults("idACM")))
      .count().toDouble
    val total: Double = groundTruthDF.count.toDouble

    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is $accuracy")
    logInfo("********************************")

    // terminate spark context
    spark.stop()
  }
}

