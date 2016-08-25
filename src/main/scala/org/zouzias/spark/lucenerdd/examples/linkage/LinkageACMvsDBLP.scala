package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

/**
 * Record linkage example between ACM and DBLP using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-acm.sh
 */
object LinkageACMvsDBLP extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageACMvsDBLP.getClass.getName)

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val acmDF = sqlContext.read.parquet("data/linkage-papers2/linkage-papers-acm.parquet")
    logInfo(s"Loaded ${acmDF.count} ACM records")
    val dblp2DF = sqlContext.read.parquet("data/linkage-papers2/linkage-papers-dblp2.parquet")
    logInfo(s"Loaded ${acmDF.count} DBLP records")
    val groundTruthDF = sqlContext.read.parquet("data/linkage-papers2/linkage-papers-acm-vs-dblp2.parquet")

    val acm = acmDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3), row.get(4).toString))
    val dblp2 = LuceneRDD(dblp2DF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3), row.get(4).toString)))
    dblp2.cache()

    // Link is the author tokens or title tokens match. Combine all tokens by an OR clause
    val linker: (String, String, String, String, String) => String = {
      case (_, title, authors, _, year) => {
        val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
        val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

        if (authorsTerms.nonEmpty) {
          s"(_2:(${titleTokens})) OR (_3:${authorsTerms})"
        }
        else{
          s"_2:(${titleTokens})"
        }
      }
    }

    val linkedResults = dblp2.link(acm, linker.tupled, 10)

    import sqlContext.implicits._

    val linkageResults = linkedResults.filter(_._2.nonEmpty).map{ case (acm, topDocs) => (topDocs.head.doc.textField("_1").head, acm._1.toInt)}.toDF("idDBLP", "idACM")

    val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idACM").equalTo(linkageResults("idACM"))).count
    val total: Double = groundTruthDF.count

    val accuracy = correctHits / total

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is ${accuracy}")
    logInfo("********************************")
    // terminate spark context
    sc.stop()

  }
}

