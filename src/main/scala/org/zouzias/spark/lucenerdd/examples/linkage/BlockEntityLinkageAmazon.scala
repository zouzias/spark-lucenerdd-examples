package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import org.apache.spark.internal.Logging

/**
 * Record linkage example between amazon and itself blocked by manufacturer using
 * [[LuceneRDD.blockEntityLinkage]] method
 *
 * You can run this locally with ./spark-blocklinkage-amazon.sh
 */
object BlockEntityLinkageAmazon extends Logging {

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
    val linker: Row => Query = { row => {

        val name = row.getString(row.fieldIndex("title"))
        val description = row.getString(row.fieldIndex("description"))
        val nameTokens = name.split(" ")
          .flatMap(_.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
          .filter(_.length > 1)
          .distinct

        val descTerms = description.split(" ")
          .flatMap(_.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
          .filter(_.length >= 2)
          .distinct

        val booleanQuery = new BooleanQuery.Builder()

        if (nameTokens.nonEmpty && descTerms.nonEmpty) {

          nameTokens.foreach{ name =>
            booleanQuery.add(new TermQuery(new Term("title", name.toLowerCase)),
              Occur.SHOULD)
          }

          descTerms.foreach{ name =>
            booleanQuery.add(new TermQuery(new Term("description", name.toLowerCase())),
              Occur.SHOULD)
          }

          booleanQuery.build()

        }
        else if (nameTokens.nonEmpty){
          nameTokens.foreach{ name =>
            booleanQuery.add(new TermQuery(new Term("title", name.toLowerCase())),
              Occur.SHOULD)
          }

          booleanQuery.build()
        }
        else {
          new MatchAllDocsQuery()
        }
      }
    }

    val blockingFields = Array("manufacturer")

    // Block entity linkage
    val linkedResults = LuceneRDD.blockEntityLinkage(amazon, amazon,
      linker, /* Link by title similarity */
      blockingFields, blockingFields)

    // Compute the performance of linkage (accuracy)
    val linkageResults: DataFrame = spark.createDataFrame(linkedResults
      .filter(_._2.nonEmpty)
      .map{ case (left, topDocs) =>
        (topDocs.head.get(topDocs.head.fieldIndex("id")),
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

