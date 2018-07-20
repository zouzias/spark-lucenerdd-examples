package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between amazon and itself blocked by manufacturer using
 * [[LuceneRDD.blockEntityLinkage]] method
 *
 * You can run this locally with ./spark-blocklinkage-amazon.sh
 */
object BlockLinkageAmazon extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageGooglevsAmazon.getClass.getName)

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.sqlContext.implicits._

    val start = System.currentTimeMillis()
    val amazonDF = spark.read.parquet("data/linkage-products1/linkage-products-amazon.parquet")
    logInfo(s"Loaded ${amazonDF.count} ACM records")

    val amazon = amazonDF.select("id", "title", "description", "manufacturer")

    // Custom linker
    val linker: Row => String = {
      case row => {
        val name = row.getString(row.fieldIndex("title"))
        val description = row.getString(row.fieldIndex("description"))
        val nameTokens = name.split(" ")
          .map(_.replaceAll("[^a-zA-Z0-9]", ""))
          .filter(_.length > 1)
          .distinct
          .mkString(" OR ")
        val descTerms = description.split(" ")
          .map(_.replaceAll("[^a-zA-Z0-9]", ""))
          .filter(_.length > 6)
          .distinct
          .mkString(" OR ")

        if (nameTokens.nonEmpty && descTerms.nonEmpty) {
          s"title:(${nameTokens}) OR description:(${descTerms})"
        }
        else if (nameTokens.nonEmpty){
          s"title:(${nameTokens})"
        }
        else {
          "*:*"
        }
      }
    }

    val blockingFields = Array("manufacturer")

    // Block entity linkage
    val linkedResults = LuceneRDD.blockEntityLinkage(amazon, amazon,
      linker, /* Link by title similarity */
      blockingFields, blockingFields)

    val linkageResults: DataFrame = spark.createDataFrame(linkedResults
      .filter(_._2.nonEmpty)
      .map{ case (left, topDocs) =>
        (topDocs.head.doc.textField("id").headOption,
          left.getString(left.fieldIndex("id"))
        )
      })
      .toDF("left_id", "right_id")
      .filter($"left_id".equalTo($"right_id"))

    val correctHits: Double = linkageResults.count()
    logInfo(s"Correct hits are $correctHits")
    val total: Double = amazonDF.count
    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"|| Elapsed time: ${(end - start) / 1000.0} seconds ||")
    logInfo("=" * 40)

    logInfo("*" * 40)
    logInfo(s"* Accuracy of deduplication is $accuracy *")
    logInfo("*" * 40)
    // terminate spark context
    spark.stop()

  }
}

