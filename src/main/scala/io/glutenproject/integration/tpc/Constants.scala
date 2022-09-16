package io.glutenproject.integration.tpc

import java.sql.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType}

object Constants {

  val VANILLA_CONF: SparkConf = new SparkConf()

  val VELOX_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "velox")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

  val GAZELLE_CPP_BACKEND_CONF: SparkConf = new SparkConf()
    .set("spark.sql.parquet.enableVectorizedReader", "true")
    .set("spark.plugins", "io.glutenproject.GlutenPlugin")
    .set("spark.gluten.sql.columnar.backend.lib", "gazelle_cpp")
    .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")

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
