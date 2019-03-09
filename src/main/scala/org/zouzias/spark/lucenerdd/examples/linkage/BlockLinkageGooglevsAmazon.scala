package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.lucene.index.Term
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.search._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.zouzias.spark.lucenerdd.{LuceneRDD, _}
import org.zouzias.spark.lucenerdd.logging.Logging
import org.apache.spark.sql.functions.{log10, round}
import org.apache.spark.sql.types.{IntegerType, StringType}

/**
 * Block Record linkage example between amazon and google product's descriptions using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-blocklinkage-products1.sh
 */
object BlockLinkageGooglevsAmazon extends Logging {

  def main(args: Array[String]) {

    // initialise spark context
    val conf = new SparkConf().setAppName(LinkageGooglevsAmazon.getClass.getName)

    implicit val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val start = System.currentTimeMillis()
    val amznDF = spark.read.parquet("data/linkage-products1/linkage-products-amazon.parquet")
    val ggleDF = spark.read.parquet("data/linkage-products1/linkage-products-google.parquet")


    logInfo(s"Loaded ${amznDF.count} ACM records")
    logInfo(s"Loaded ${ggleDF.count} DBLP records")

    val groundTruthDF = spark.read.parquet("data/linkage-products1/linkage-products-amazon-vs-google.parquet")

    val amazonDF = amznDF.withColumn("priceDigits", round(log10('price + 1)).cast(StringType))
    val googleDF = ggleDF.withColumn("priceDigits", round(log10('price + 1)).cast(StringType))

    // Custom linker
    val linker: Row => Query = {
      case row => {

        val name = row.getString(row.fieldIndex("name"))
        val description = row.getString(row.fieldIndex("description"))

        // Clean fields and tokenize them
        val nameTokens = name.split(" ")
          .map(_.replaceAll("[^a-zA-Z0-9]", ""))
          .filter(_.length > 1)
          .distinct

        val booleanQuery = new BooleanQuery.Builder()

        nameTokens.foreach{ name =>
          booleanQuery.add(new TermQuery(new Term("title", name)), Occur.SHOULD)
        }

        /*
        val descTerms = description.split(" ")
          .map(_.replaceAll("[^a-zA-Z0-9]", ""))
          .filter(_.length > 6)
          .distinct
          .mkString(" OR ")
        */

        if (nameTokens.nonEmpty) {
          booleanQuery.build()
        }
        else {
          new MatchAllDocsQuery()
        }
      }
    }


    val blockingFields = Array("priceDigits")

    // Block entity linkage
    val linkedResults = LuceneRDD.blockEntityLinkage(googleDF, amazonDF,
      linker, /* Link by title similarity */
      blockingFields, blockingFields)

    val linkageResults = spark.createDataFrame(linkedResults
      .map{ case (left, topDocs) => (topDocs.headOption.flatMap(_.doc.textField("_1")), left.getString(0))})
      .toDF("idGoogleBase", "idAmazon")

    val correctHits: Double = linkageResults
      .join(groundTruthDF, groundTruthDF.col("idAmazon").equalTo(linkageResults("idAmazon")) &&  groundTruthDF.col("idGoogleBase").equalTo(linkageResults("idGoogleBase")))
      .count()
    val total: Double = groundTruthDF.count()
    val accuracy = correctHits / total
    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo("*" * 40)
    logInfo(s"Accuracy of linkage is $accuracy")
    logInfo("*" * 40)

    // terminate spark context
    spark.stop()
  }
}

