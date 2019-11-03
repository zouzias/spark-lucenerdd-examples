package org.zouzias.spark.lucenerdd.examples.linkage

import org.apache.spark.sql.DataFrame

object LinkageUtils {

  def matches(linkageResults: DataFrame, truthDF: DataFrame,
              leftId: String, rightId: String)
  : Long = {
    linkageResults
      .join(truthDF, truthDF.col(leftId).equalTo(linkageResults(leftId))
        && truthDF.col(rightId).equalTo(linkageResults(rightId)))
      .count()
  }

  /**
    * Naive white-space tokenizer for text, keep only alphanumerics
    *
    * @param text
    * @param minThreshold Keep tokens with length more than minThreshold
    * @return Array of tokens / words
    */
  def tokenize(text: String, minThreshold: Int): Array[String] = {
    text.split(" ")
      .flatMap(_.replaceAll("[^a-zA-Z0-9]", " ").split(" "))
      .filter(_.length > minThreshold)
      .distinct
  }
}
