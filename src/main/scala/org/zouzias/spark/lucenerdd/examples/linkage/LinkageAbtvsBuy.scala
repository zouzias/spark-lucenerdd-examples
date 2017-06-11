package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.zouzias.spark.lucenerdd.LuceneRDD
import org.zouzias.spark.lucenerdd._
import org.zouzias.spark.lucenerdd.logging.Logging

/**
 * Record linkage example between Abt and Buy product's descriptions using [[LuceneRDD]]
 *
 * You can run this locally with, ./spark-linkage-products2.sh
 */
object LinkageAbtvsBuy extends Logging {

  def main(args: Array[String]) {

    // initialise sparkSession context
    val conf = new SparkConf().setAppName(LinkageAbtvsBuy.getClass.getName)

    implicit val sc = SparkSession.builder().config(conf).getOrCreate()
    import sc.implicits._

    val start = System.currentTimeMillis()

    val abtDF = sc.read.parquet("data/linkage-products2/linkage-products-abt.parquet")
    logInfo(s"Loaded ${abtDF.count} Abt product descriptions")
    val buyDF = sc.read.parquet("data/linkage-products2/linkage-products-buy.parquet")
    logInfo(s"Loaded ${buyDF.count} Buy product descriptions")
    val groundTruthDF = sc.read.parquet("data/linkage-products2/linkage-products-abt-vs-buy.parquet")


    val abt = abtDF.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3)))
    val buy = LuceneRDD(buyDF.rdd.map( row => (row.get(0).toString, row.getString(1), row.getString(2), row.getString(3))))


    val linker: (String, String, String, String) => String = {
      case (_, name, description, _) => {
        val nameTokens = name.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 0).mkString(" OR ")
        val descTerms = description.split(" ").map(_.replaceAll("[^a-zA-Z0-9]", "")).filter(_.length > 0).mkString(" OR ")

        if (descTerms.nonEmpty) {
          s"(_2:(${nameTokens})) OR (_3:${descTerms})"
        }
        else{
          s"_2:(${nameTokens})"
        }
      }
    }


    val linkedResults = buy.link(abt.rdd, linker.tupled, 3)

    val linkageResultsIds = sc.createDataFrame(linkedResults.filter(_._2.nonEmpty).map{ case (abtId, topDocs) => (topDocs.head.doc.textField("_1").head, abtId._1.toInt)}).toDF("idBuy", "idAbt")

    val correctHits: Double = linkageResultsIds.join(groundTruthDF, groundTruthDF.col("idAbt").equalTo(linkageResultsIds("idAbt")) &&  groundTruthDF.col("idBuy").equalTo(linkageResultsIds("idBuy"))).count
    val total: Double = groundTruthDF.count

    val accuracy = correctHits / total

    val end = System.currentTimeMillis()

    logInfo("=" * 40)
    logInfo(s"Elapsed time: ${(end - start) / 1000.0} seconds")
    logInfo("=" * 40)

    logInfo("********************************")
    logInfo(s"Accuracy of linkage is ${accuracy}")
    logInfo("********************************")
    // terminate sparkSession context
    sc.stop()

  }
}

