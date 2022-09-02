package io.glutenproject.integration.tpc

import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType}

object Constants {

  val VANILLA_CONF: SparkConf = new SparkConf()
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.sql.codegen.wholeStage", "false")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.sql.broadcastTimeout", "1800")
    .set("spark.network.io.preferDirectBufs", "false")

  val VELOX_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "velox")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.codegen.wholeStage", "true")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.sql.broadcastTimeout", "1800")
    .set("spark.network.io.preferDirectBufs", "false")
    .set("spark.gluten.sql.columnar.columnartorow", "false")

  val GAZELLE_CPP_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "gazelle_cpp")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    .set("spark.sql.codegen.wholeStage", "true")
    .set("spark.sql.sources.useV1SourceList", "")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3600000")
    .set("spark.executor.heartbeatInterval", "3600000")
    .set("spark.network.timeout", "3601s")
    .set("spark.sql.broadcastTimeout", "1800")
    .set("spark.network.io.preferDirectBufs", "false")
    .set("spark.gluten.sql.columnar.columnartorow", "false")

  val TYPE_MODIFIER_DATE_AS_DOUBLE: TypeModifier = new TypeModifier(DateType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Date => v.getTime.asInstanceOf[Double] / 86400.0D / 1000.0D
      }
    }
  }

  val TYPE_MODIFIER_INTEGER_AS_DOUBLE: TypeModifier = new TypeModifier(IntegerType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Int => v.asInstanceOf[Double]
      }
    }
  }

  val TYPE_MODIFIER_LONG_AS_DOUBLE: TypeModifier = new TypeModifier(LongType, DoubleType) {
    override def modValue(from: Any): Any = {
      from match {
        case v: Long => v.asInstanceOf[Double]
      }
    }
  }
}
