package io.glutenproject.integration.tpc.h

import io.glutenproject.integration.stat.RamStat
import io.glutenproject.integration.tpc.TpcRunner
import io.glutenproject.integration.tpc.h.TpchSuite.{ALL_QUERY_IDS, TestResultLine}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.HistoryServerHelper
import org.apache.spark.sql.{GlutenSparkSessionSwitcher, GlutenTestUtils}
import java.io.File
import java.util.Scanner

import scala.collection.JavaConverters._

class TpchSuite(
    val testConf: SparkConf,
    val baselineConf: SparkConf,
    val scale: Double,
    val typeModifiers: java.util.List[TypeModifier],
    val queryResource: String,
    val queryIds: Array[String],
    val logLevel: Level,
    val explain: Boolean,
    val errorOnMemLeak: Boolean,
    val enableHsUi: Boolean,
    val hsUiPort: Int,
    val cpus: Int,
    val offHeapSize: String,
    val iterations: Int) {

  System.setProperty("spark.testing", "true")
  resetLogLevel()

  private val sessionSwitcher: GlutenSparkSessionSwitcher = new GlutenSparkSessionSwitcher(cpus)
  private val runner: TpcRunner = new TpcRunner(queryResource, TpchSuite.TPCH_WRITE_PATH)

  // define initial configs
  sessionSwitcher.defaultConf().set("spark.unsafe.exceptionOnMemoryLeak", s"$errorOnMemLeak")
  sessionSwitcher.defaultConf().set("spark.ui.enabled", "false")
  sessionSwitcher.defaultConf().set("spark.memory.offHeap.size", offHeapSize)

  if (enableHsUi) {
    if (!new File(TpchSuite.HISTORY_WRITE_PATH).mkdirs()) {
      throw new RuntimeException("Unable to create history directory: " +
          TpchSuite.HISTORY_WRITE_PATH)
    }
    sessionSwitcher.defaultConf().set("spark.eventLog.enabled", "true")
    sessionSwitcher.defaultConf().set("spark.eventLog.dir", TpchSuite.HISTORY_WRITE_PATH)
  }

  // register sessions
  sessionSwitcher.registerSession("test", testConf)
  sessionSwitcher.registerSession("baseline", baselineConf)

  def startHistoryServer(): Unit = {
    if (!enableHsUi) {
      return
    }
    val conf = new SparkConf()
    conf.remove("spark.testing")
    conf.set("spark.history.ui.port", s"$hsUiPort")
    conf.set("spark.history.fs.logDirectory", TpchSuite.HISTORY_WRITE_PATH)
    HistoryServerHelper.startHistoryServer(conf)
  }

  def run(): Boolean = {
    // boot up history server
    startHistoryServer()

    // use vanilla spark to generate data
    resetLogLevel() // to prevent log level from being set by unknown external codes
    sessionSwitcher.useSession("baseline", "Data Gen")
    val dataGen = new TpchDataGen(sessionSwitcher.spark(), scale, cpus, TpchSuite.TPCH_WRITE_PATH,
      typeModifiers.asScala.toArray)
    dataGen.gen()

    // run tests
    resetLogLevel() // to prevent log level from being set by unknown external codes

    val results = (0 until iterations).flatMap { iteration =>
      println(s"Running tests (iteration $iteration)...")
      queryIds.map { queryId =>
        if (!ALL_QUERY_IDS.contains(queryId)) {
          throw new IllegalArgumentException(s"Query ID doesn't exist: $queryId")
        }
        runTpchQuery(queryId)
      }
    }.toList
    sessionSwitcher.close()
    val passedCount = results.count(l => l.testPassed)
    val count = results.count(_ => true)

    // RAM stats
    println("Performing GC to collect RAM statistics... ")
    System.gc()
    System.gc()
    printf("RAM statistics: JVM Heap size: %d KiB (total %d KiB), Process RSS: %d KiB\n",
      RamStat.getJvmHeapUsed(), RamStat.getJvmHeapTotal(), RamStat.getProcessRamUsed())

    println("")
    println("Test report: ")
    println("")
    printf("Summary: %d out of %d queries passed. \n", passedCount, count)
    println("")
    val succeed = results.filter(_.testPassed)
    printResults(succeed)
    println("")

    if (passedCount == count) {
      println("No failed queries. ")
      println("")
    } else {
      println("Failed queries (a failed query with correct row count indicates value mismatches): ")
      println("")
      printResults(results.filter(!_.testPassed))
      println("")
    }

    var all = aggregate(results, "all")

    if (passedCount != count) {
      all = aggregate(succeed, "all succeed") ::: all
    }

    println("Overall: ")
    println("")
    printResults(all)
    println("")

    // wait for input, if history server was started
    if (enableHsUi) {
      printf("History server was running at port %d. Press enter to exit... \n", hsUiPort)
      print("> ")
      new Scanner(System.in).nextLine
    }

    if (passedCount != count) {
      return false
    }
    true
  }

  private def aggregate(succeed: List[TestResultLine], name: String) = {
    List(succeed.reduce((r1, r2) => TestResultLine(name, testPassed = true,
      if (r1.expectedRowCount.nonEmpty && r2.expectedRowCount.nonEmpty)
        Some(r1.expectedRowCount.get + r2.expectedRowCount.get)
      else None,
      if (r1.actualRowCount.nonEmpty && r2.actualRowCount.nonEmpty)
        Some(r1.actualRowCount.get + r2.actualRowCount.get)
      else None,
      if (r1.expectedExecutionTimeMillis.nonEmpty && r2.expectedExecutionTimeMillis.nonEmpty)
        Some(r1.expectedExecutionTimeMillis.get + r2.expectedExecutionTimeMillis.get)
      else None,
      if (r1.actualExecutionTimeMillis.nonEmpty && r2.actualExecutionTimeMillis.nonEmpty)
        Some(r1.actualExecutionTimeMillis.get + r2.actualExecutionTimeMillis.get)
      else None, None)))
  }

  private def resetLogLevel(): Unit = {
    LogManager.getRootLogger.setLevel(logLevel)
  }

  private def printResults(results: List[TestResultLine]) = {
    printf("|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n", "Query ID", "Was Passed", "Expected Row Count",
      "Actual Row Count", "Baseline Query Time (Millis)", "Query Time (Millis)", "Query Time Variation")
    results.foreach { line =>
      val timeVariation =
        if (line.expectedExecutionTimeMillis.nonEmpty && line.actualExecutionTimeMillis.nonEmpty) {
          Some(((line.expectedExecutionTimeMillis.get - line.actualExecutionTimeMillis.get).toDouble
            / line.actualExecutionTimeMillis.get.toDouble) * 100)
        } else None
      printf("|%15s|%15s|%30s|%30s|%30s|%30s|%30s|\n", line.queryId, line.testPassed,
        line.expectedRowCount.getOrElse("N/A"),
        line.actualRowCount.getOrElse("N/A"),
        line.expectedExecutionTimeMillis.getOrElse("N/A"),
        line.actualExecutionTimeMillis.getOrElse("N/A"),
        timeVariation.map("%15.2f%%".format(_)).getOrElse("N/A"))
    }
  }

  private def runTpchQuery(id: String): TestResultLine = {
    println(s"Running query: $id...")
    try {
      val baseLineDesc = "Vanilla Spark TPC-H %s".format(id)
      sessionSwitcher.useSession("baseline", baseLineDesc)
      runner.createTables(sessionSwitcher.spark())
      val expected = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain,
        baseLineDesc)
      val expectedRows = expected.rows
      val testDesc = "Gluten Spark TPC-H %s".format(id)
      sessionSwitcher.useSession("test", testDesc)
      runner.createTables(sessionSwitcher.spark())
      val result = runner.runTpcQuery(sessionSwitcher.spark(), id, explain = explain,
        testDesc)
      val resultRows = result.rows
      val error = GlutenTestUtils.compareAnswers(resultRows, expectedRows, sort = true)
      if (error.isEmpty) {
        println(s"Successfully ran query $id, result check was passed. " +
            s"Returned row count: ${resultRows.length}, expected: ${resultRows.length}")
        return TestResultLine(id, testPassed = true, Some(resultRows.length), Some(resultRows.length),
          Some(expected.executionTimeMillis), Some(result.executionTimeMillis), None)
      }
      println(s"Error running query $id, result check was not passed. " +
          s"Returned row count: ${resultRows.length}, expected: ${resultRows.length}, error: ${error.get}")
      TestResultLine(id, testPassed = false, Some(resultRows.length), Some(resultRows.length),
        Some(expected.executionTimeMillis), Some(result.executionTimeMillis), error)
    } catch {
      case e: Exception =>
        val error = Some(s"FATAL: ${ExceptionUtils.getStackTrace(e)}")
        println(s"Error running query $id. " +
            s" Error: ${error.get}")
        TestResultLine(id, testPassed = false, None, None, None, None, error)
    }
  }
}

object TpchSuite {
  private val TPCH_WRITE_PATH = "/tmp/tpch-generated"
  private val ALL_QUERY_IDS = Set("q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
    "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", "q20", "q21", "q22")

  private val HISTORY_WRITE_PATH = "/tmp/tpch-history"

  case class TestResultLine(
      queryId: String,
      testPassed: Boolean,
      expectedRowCount: Option[Long],
      actualRowCount: Option[Long],
      expectedExecutionTimeMillis: Option[Long],
      actualExecutionTimeMillis: Option[Long],
      errorMessage: Option[String])
}
