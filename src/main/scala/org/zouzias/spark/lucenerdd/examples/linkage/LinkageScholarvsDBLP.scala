package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between Google scholar and DBLP using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-scholar.sh
 */
object LinkageScholarvsDBLP extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageScholarvsDBLP.getClass.getName)

    implicit val sc = SparkSession.builder.config(conf).getOrCreate()
    import sc.implicits._

    val start = System.currentTimeMillis()
    val scholarDF = sc.read.parquet("data/linkage-papers1/linkage-papers-scholar.parquet")
    logInfo(s"Loaded ${scholarDF.count} ACM records")
    val dblpDF = sc.read.parquet("data/linkage-papers1/linkage-papers-dblp.parquet")
    logInfo(s"Loaded ${scholarDF.count} DBLP records")
    val groundTruthDF = sc.read.parquet("data/linkage-papers1/linkage-papers-scholar-vs-dblp.parquet")

    val scholar = scholarDF.select("id", "title", "authors", "venue")

    val dblp = LuceneRDD(dblpDF)

    // A custom linker
    val linker: Row => String = {
      case row => {
        val title = row.getString(row.fieldIndex("title"))
        val authors = row.getString(row.fieldIndex("authors"))

        val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
        val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

        if (titleTokens.nonEmpty && authorsTerms.nonEmpty) {
          s"(title:(${titleTokens}) OR authors:(${authorsTerms}))"
        }
        else if (titleTokens.nonEmpty){
          s"title:(${titleTokens})"
        }
        else if (authorsTerms.nonEmpty){
          s"authors:(${authorsTerms})"
        }
        else {
          "*:*"
        }
      }
    }

    val linkedResults = dblp.linkDataFrame(scholar, linker, 3)

    val linkageResults = sc.createDataFrame(linkedResults.filter(_._2.nonEmpty).map{ case (scholar, topDocs) => (topDocs.head.doc.textField("id").head, scholar.getString(scholar.fieldIndex("id")))})
      .toDF("idDBLP", "idScholar")

    val correctHits: Double = linkageResults
      .join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idScholar").equalTo(linkageResults("idScholar"))).count
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

