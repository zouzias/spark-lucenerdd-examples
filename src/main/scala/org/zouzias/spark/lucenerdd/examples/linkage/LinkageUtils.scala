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
}
