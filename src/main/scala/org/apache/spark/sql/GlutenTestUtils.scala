package org.apache.spark.sql

import org.apache.spark.sql.test.SQLTestUtils

object GlutenTestUtils {
  def compareAnswers(
      sparkAnswer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    SQLTestUtils.compareAnswers(sparkAnswer, expectedAnswer, sort)
  }
}
