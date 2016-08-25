package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._

/**
 * Record linkage example between Google scholar and DBLP using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-scholar.sh
 */
object LinkageScholarvsDBLP extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageScholarvsDBLP.getClass.getName)

    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val scholarDF = sqlContext.read.parquet("data/linkage-papers1/linkage-papers-scholar.parquet")
    logInfo(s"Loaded ${scholarDF.count} ACM records")
    val dblpDF = sqlContext.read.parquet("data/linkage-papers1/linkage-papers-dblp.parquet")
    logInfo(s"Loaded ${scholarDF.count} DBLP records")
    val groundTruthDF = sqlContext.read.parquet("data/linkage-papers1/linkage-papers-scholar-vs-dblp.parquet")

    val scholar = scholarDF.select("id", "title", "authors", "venue").map( row => (row.get(0).toString, row.getString(1), row.getString(2)))
    val dblp = LuceneRDD(dblpDF.select("id", "title", "authors", "venue").map( row => (row.get(0).toString, row.getString(1), row.getString(2))))

    // A custom linker
    val linker: (String, String, String) => String = {
      case (_, title, authors) => {
        val titleTokens = title.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 3).mkString(" OR ")
        val authorsTerms = authors.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 2).mkString(" OR ")

        if (titleTokens.nonEmpty && authorsTerms.nonEmpty) {
          s"(_2:(${titleTokens}) OR _3:(${authorsTerms}))"
        }
        else if (titleTokens.nonEmpty){
          s"_2:(${titleTokens})"
        }
        else if (authorsTerms.nonEmpty){
          s"_3:(${authorsTerms})"
        }
        else {
          "*:*"
        }
      }
    }

    val linkedResults = dblp.link(scholar, linker.tupled, 3)

    import sqlContext.implicits._

    val linkageResults = linkedResults.filter(_._2.nonEmpty).map{ case (scholar, topDocs) => (topDocs.head.doc.textField("_1").head, scholar._1)}.toDF("idDBLP", "idScholar")

    val correctHits: Double = linkageResults.join(groundTruthDF, groundTruthDF.col("idDBLP").equalTo(linkageResults("idDBLP")) &&  groundTruthDF.col("idScholar").equalTo(linkageResults("idScholar"))).count
    val total: Double = groundTruthDF.count
    val accuracy = correctHits / total

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is ${accuracy}")
    logInfo("********************************")
    // terminate spark context
    sc.stop()

  }
}

