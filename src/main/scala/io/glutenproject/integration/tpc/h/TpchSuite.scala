package io.glutenproject.integration.tpc.h

import io.glutenproject.integration.tpc.TpcRunner
import io.glutenproject.integration.tpc.h.TpchSuite.{ALL_QUERY_IDS, TestResultLine}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils}

import scala.collection.JavaConverters._

class TpchSuite(
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val scale: Double,
    val typeModifiers: java.util.List[TypeModifier],
    val queryResource: String,
    val queryIds: Array[String],
    val logLevel: Level) {

  System.setProperty("spark.testing", "true")
  LogManager.getRootLogger.setLevel(logLevel)

  private val sessionSwitcher: GlutenSparkSessionSwitcher = new GlutenSparkSessionSwitcher()
  private val runner: TpcRunner = new TpcRunner(queryResource, TpchSuite.TPCH_WRITE_PATH)

  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)
  sessionSwitcher.useSession("baseline") // use vanilla spark to generate data

  private val dataGen = new TpchDataGen(sessionSwitcher.spark(), scale, TpchSuite.TPCH_WRITE_PATH,
    typeModifiers.asScala.toArray)

  def run(): Boolean = {
    dataGen.gen()
    val results = queryIds.map { queryId =>
      if (!ALL_QUERY_IDS.contains(queryId)) {
        throw new IllegalArgumentException(s"Query ID doesn't exist: $queryId")
      }
      runTpchQuery(queryId)
    }.toList
    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)
    println("")
    println("Test report:")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    printf("|%15s|%15s|%30s|%30s|\n", "Query ID", "Was Passed", "Expected Row Count",
      "Actual Row Count")
    results.foreach { line =>
      printf("|%15s|%15s|%30s|%30s|\n", line.queryId, line.testPassed,
        line.expectedRowCount.getOrElse("N/A"),
        line.actualRowCount.getOrElse("N/A"))
    }
    if (passedCount != count) {
      return false
    }
    true
  }

  private def runTpchQuery(id: String): TestResultLine = {
    println(s"Running query: $id...")
    try {
      sessionSwitcher.useSession("baseline")
      runner.createTables(sessionSwitcher.spark())
      val expected = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
      sessionSwitcher.useSession("test")
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = true)
      val error = GlutenTestUtils.compareAnswers(result, expected, sort = true)
      if (error.isEmpty) {
        println(s"Successfully ran query $id, result check was passed. " +
            s"Returned row count: ${result.length}, expected: ${expected.length}")
        return TestResultLine(id, testPassed = true, Some(expected.length), Some(result.length), None)
      }
      TestResultLine(id, testPassed = false, Some(expected.length), Some(result.length), error)
    } catch {
      case e: Exception =>
        TestResultLine(id, testPassed = false, None, None, Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}"))
    }
  }
}

object TpchSuite {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"
  private val ALL_QUERY_IDS = Set("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")

  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      expectedRowCount: Option[Long],
      actualRowCount: Option[Long],
      errorMessage: Option[String])
}
