package io.glutenproject.integration.tpc.h;

import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


@CommandLine.Command(name = "gluten-tpch", mixinStandardHelpOptions = true,
    description = "Gluten integration test using TPC-H benchmark's data and queries")
public class Tpch implements Callable<Integer> {

  @CommandLine.Option(required = true, names = {"-b", "--backend-type"}, description = "Backend used: velox, gazelle-cpp, ...")
  private String backendType;

  @CommandLine.Option(names = {"-s", "--scale"}, description = "The scale factor of sample TPC-H dataset", defaultValue = "0.1")
  private double scale;

  @CommandLine.Option(names = {"--fixed-width-as-double"}, description = "Generate integer/long/date as double", defaultValue = "false")
  private boolean fixedWidthAsDouble;

  @CommandLine.Option(names = {"--queries"}, description = "Set a comma-seperated list of query IDs to run", split = ",", defaultValue = "q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14,q15,q16,q17,q18,q19,q20,q21,q22")
  private String[] queries;

  @CommandLine.Option(names = {"--log-level"}, description = "Set log level: 0 for DEBUG, 1 for INFO, 2 for WARN", defaultValue = "2")
  private int logLevel;

  @CommandLine.Option(names = {"--explain"}, description = "Output explain result for queries", defaultValue = "false")
  private boolean explain;

  @Override
  public Integer call() throws Exception {
    final SparkConf baselineConf = io.glutenproject.integration.tpc.h.package$.MODULE$.VANILLA_CONF();
    final SparkConf testConf;
    switch (backendType) {
      case "velox":
        testConf = package$.MODULE$.VELOX_BACKEND_CONF();
        break;
      case "gazelle-cpp":
        testConf = package$.MODULE$.GAZELLE_CPP_BACKEND_CONF();
        break;
      default:
        throw new IllegalArgumentException("Backend type not found: " + backendType);
    }
    final List<TypeModifier> typeModifiers = new ArrayList<>();
    final String queryResource;
    if (fixedWidthAsDouble) {
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_INTEGER_AS_DOUBLE());
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_LONG_AS_DOUBLE());
      typeModifiers.add(package$.MODULE$.TYPE_MODIFIER_DATE_AS_DOUBLE());
      queryResource = "/tpch-queries-noint-nodate";
    } else {
      queryResource = "/tpch-queries";
    }
    final Level level;
    switch (logLevel) {
      case 0:
        level = Level.DEBUG;
        break;
      case 1:
        level = Level.INFO;
        break;
      case 2:
        level = Level.WARN;
        break;
      default:
        throw new IllegalArgumentException("Log level not found: " + logLevel);
    }
    final TpchSuite suite = new TpchSuite(testConf, baselineConf, scale,
        typeModifiers, queryResource, queries, level, explain);
    if (!suite.run()) {
      return -1;
    }
    return 0;
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new Tpch()).execute(args);
    System.exit(exitCode);
  }
}
