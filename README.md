# Gluten Itegration Testing (Gluten-IT)

The project makes it easy to test Gluten build locally.

## Gluten ?

Gluten is a native Spark SQL implementation as a standard Spark plug-in.

https://github.com/oap-project/gluten

## Getting Started

### 1. Install Gluten in your local machine

See offical Gluten build guidence https://github.com/oap-project/gluten#how-to-use-gluten

### 2. Install and run Gluten-IT

```sh
git clone -b main https://github.com/zhztheplayer/gluten-it.git gluten-it
cd gluten-it
mvn clean package
java -Xmx5G -cp target/gluten-it-1.0-SNAPSHOT-jar-with-dependencies.jar io.glutenproject.integration.tpc.h.Tpch
```

## Usage

### CMD args

```
Usage: gluten-tpch [-hV] [--enable-history] [--error-on-memleak] [--explain]
                   [--fixed-width-as-double] -b=<backendType>
                   [--baseline-backend-type=<baselineBackendType>]
                   [--cpus=<cpus>] [--history-ui-port=<hsUiPort>]
                   [--iterations=<iterations>] [--log-level=<logLevel>]
                   [--off-heap-size=<offHeapSize>] [-s=<scale>]
                   [--queries=<queries>[,<queries>...]]...
Gluten integration test using TPC-H benchmark's data and queries
  -b, --backend-type=<backendType>
                           Backend used: vanilla, velox, gazelle-cpp, ...
      --baseline-backend-type=<baselineBackendType>
                           Baseline backend used: vanilla, velox, gazelle-cpp,
                             ...
                             Default: vanilla
      --cpus=<cpus>        Executor cpu number
                             Default: 2
      --enable-history     Start a Spark history server during running
      --error-on-memleak   Fail the test when memory leak is detected by
                             Spark's memory manager
      --explain            Output explain result for queries
      --fixed-width-as-double
                           Generate integer/long/date as double
  -h, --help               Show this help message and exit.
      --history-ui-port=<hsUiPort>
                           Port that Spark history server UI binds to
                             Default: 18080
      --iterations=<iterations>
                           How many iterations to run
                             Default: 1
      --log-level=<logLevel>
                           Set log level: 0 for DEBUG, 1 for INFO, 2 for WARN
                             Default: 2
      --off-heap-size=<offHeapSize>
                           Off heap memory size per executor
                             Default: 6g
      --queries=<queries>[,<queries>...]
                           Set a comma-seperated list of query IDs to run
                             Default: q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,
                             q13,q14,q15,q16,q17,q18,q19,q20,q21,q22
  -s, --scale=<scale>      The scale factor of sample TPC-H dataset
                             Default: 0.1
  -V, --version            Print version information and exit.
```
