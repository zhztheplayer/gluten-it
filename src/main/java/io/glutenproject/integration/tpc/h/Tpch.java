package io.glutenproject.integration.tpc.h;

import io.glutenproject.integration.tpc.TypeModifier;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;


@CommandLine.Command(name = "gluten-tpch", mixinStandardHelpOptions = true,
    showDefaultValues = true,
    description = "Gluten integration test using TPC-H benchmark's data and queries")
public class Tpch implements Callable<Integer> {

  @CommandLine.Option(required = true, names = {"-b", "--backend-type"}, description = "Backend used: vanilla, velox, gazelle-cpp, ...")
  private String backendType;

  @CommandLine.Option(required = true, names = {"--baseline-backend-type"}, description = "Baseline backend used: vanilla, velox, gazelle-cpp, ...", defaultValue = "vanilla")
  private String baselineBackendType;

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

  @CommandLine.Option(names = {"--error-on-memleak"}, description = "Fail the test when memory leak is detected by Spark's memory manager", defaultValue = "false")
  private boolean errorOnMemLeak;

  @CommandLine.Option(names = {"--enable-history"}, description = "Start a Spark history server during running", defaultValue = "false")
  private boolean enableHsUi;

  @CommandLine.Option(names = {"--history-ui-port"}, description = "Port that Spark history server UI binds to", defaultValue = "18080")
  private int hsUiPort;

  @CommandLine.Option(names = {"--cpus"}, description = "Executor cpu number", defaultValue = "2")
  private int cpus;

  @CommandLine.Option(names = {"--off-heap-size"}, description = "Off heap memory size per executor", defaultValue = "6g")
  private String offHeapSize;

  @CommandLine.Option(names = {"--iterations"}, description = "How many iterations to run", defaultValue = "1")
  private int iterations;

  private SparkConf pickSparkConf(String backendType) {
    SparkConf conf;
    switch (backendType) {
      case "vanilla":
        conf = package$.MODULE$.VANILLA_CONF();
        break;
      case "velox":
        conf = package$.MODULE$.VELOX_BACKEND_CONF();
        break;
      case "gazelle-cpp":
        conf = package$.MODULE$.GAZELLE_CPP_BACKEND_CONF();
        break;
      default:
        throw new IllegalArgumentException("Backend type not found: " + backendType);
    }
    return conf;
  }

  @Override
  public Integer call() throws Exception {
    final SparkConf baselineConf = pickSparkConf(baselineBackendType);
    final SparkConf testConf = pickSparkConf(backendType);
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
        typeModifiers, queryResource, queries, level, explain, errorOnMemLeak,
        enableHsUi, hsUiPort, cpus, offHeapSize, iterations);
    boolean succeed = suite.run();
    if (!succeed) {
      return -1;
    }
    return 0;
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new Tpch()).execute(args);
    System.exit(exitCode);
  }
}
