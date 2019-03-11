package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd.logging.Logging


/**
 * Record linkage example between amazon and itself blocked by manufacturer using
 * [[LuceneRDD.blockEntityLinkage]] method
 *
 * You can run this locally with ./spark-blocklinkage-amazon.sh
 */
object BlockDedupIllinois extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageGooglevsAmazon.getClass.getName)

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.sqlContext.implicits._

    val start = System.currentTimeMillis()
    val illinoisFullDF = spark.read.parquet("data/illinois-donors-10K-sample.parquet")
    logInfo(s"Loaded ${illinoisFullDF.count} records")

    val illinoisDF = illinoisFullDF.select("RctNum", "LastOnlyName", "FirstName", "City")


    // Custom linker
    val linker: Row => Query = { row => {

        val name = row.getString(row.fieldIndex("FirstName"))
        val lastName = row.getString(row.fieldIndex("LastOnlyName"))


      val booleanQuery = new BooleanQuery.Builder()
        if (name != null) {
          name.split(" ")
            .flatMap(x => x.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
            .filter(_.length >= 2).foreach { nameToken =>
            booleanQuery.add(new TermQuery(new Term("FirstName", nameToken.toLowerCase)), Occur.SHOULD)
          }
        }

        if ( lastName != null) {
          lastName.split(" ")
            .flatMap(x => x.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
            .filter(_.length >= 2).foreach { lastNameToken =>
            booleanQuery.add(new TermQuery(new Term("LastOnlyName", lastNameToken.toLowerCase)), Occur.SHOULD)
          }
        }

        booleanQuery.setMinimumNumberShouldMatch(1)
        booleanQuery.build()
      }
    }

    val blockingFields = Array("City")

    // Block entity linkage
    val linkedResults = LuceneRDD.blockDedup(illinoisDF, linker, blockingFields)

    val collected = linkedResults.collect()

    collected.map(x => (x._1, x._2.headOption)).foreach(println)



    val linkageResults: DataFrame = spark.createDataFrame(linkedResults
      .filter(_._2.nonEmpty)
      .map{ case (left, topDocs) =>
        (topDocs.head.doc.textField("RctNum").headOption,
          left.getString(left.fieldIndex("RctNum"))
        )
      })
      .toDF("left_id", "right_id")
      .filter($"left_id".equalTo($"right_id"))

    val correctHits: Double = linkageResults.count()
    logInfo(s"Correct hits are $correctHits")
    val total: Double = illinoisDF.count
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

