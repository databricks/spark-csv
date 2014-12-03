# Spark SQL CSV Library

A library for parsing and querying CSV data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

## Requirements

This library requires Spark 1.2+

## Linking
You can link against this library in your program at the following coordiates:

```
groupId: com.databricks.spark
artifactId: spark-csv_2.10
version: 0.1
```
The spark-csv jar file can also be added to a Spark using the `--jars` command line option.  For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars spark-csv_2.11-0.1.jar
```

## Features
These examples use a CSV file available for download [here](https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv):

```
$ wget https://github.com/databricks/spark-csv/raw/master/src/test/resources/cars.csv
```

### Scala API

You can use the library by loading the implicits from `com.databricks.spark.csv._`.

```
scala> import org.apache.spark.sql.SQLContext

scala> val sqlContext = new SQLContext(sc)

scala> import com.databricks.spark.csv._

scala> val cars = sqlContext.csvFile("cars.csv")
```

### SQL API
CSV data can be queried in pure SQL by registering the data as a temporary table.

```sql
CREATE TEMPORARY TABLE cars
USING com.databricks.spark.csv
OPTIONS (path "cars.csv", header true)
```

### Java API
CSV files can be read using functions in CsvUtils.

```java
import com.databricks.spark.csv.CSVUtils;

JavaSchemaRDD cars = (new CsvUtils()).setUseHeader(true).csvFile(sqlContext, "cars.csv");
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html), which is automatically downloaded by the included shell script.  To build a JAR file simply run `sbt/sbt package` from the project root.

