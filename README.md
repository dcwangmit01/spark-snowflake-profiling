
The code in this benchmark repository runs 4 implementations of a Spark job submitted to a local docker-composed Spark
cluster.  Each implementation of the spark job queryies data from Snowflake in different ways, providing a benchmark
for identifying the best method.

## TLDR

Using the Snowflake Spark Driver without setting any special parameters will result in a parallel data pull far
superior to the Snowflake JDBC Driver with parallelization parameters set.  This is the best method.

## Summary of methods and findings:

### Snowflake Spark Driver - Sequential Pull

* Based on Example
  * https://docs.snowflake.com/en/user-guide/spark-connector-use.html#sample-python-script
* Results
  * SparkUI shows that the data pull occurs in parallel even though the code has no parallelization parameters set


### Snowflake Spark Driver - Parallel Pull

* Based on Example
  * https://docs.snowflake.com/en/user-guide/spark-connector-use.html#sample-python-script
  * While setting parallelization parameters (partitionColumn, lowerBound, upperBound, numPartitions) found here:
    * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
* Results
  * The results were exactly the same as using the Snowflake Spark Driver to do a Sequential Pull test.
  * SparkUI shows that the data pull occurs in parallel even though the code has no parallelization parameters set
  * The snowflake drive seems to ignore the paralleization parameters provided.

### Snowflake JDBC Driver - Sequential Pull

* Based on Example
  * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
  * Did not provide parallization parameters (partitionColumn, lowerBound, upperBound, numPartitions)
* Results
  * As expected, only a single Spark worker pulled from Snowflake.
  * No parallelization occured, and this was certainly the slowest method.

### Snowflake JDBC Driver - Parallel Pull

* Based on Example
  * https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
  * Did indeed provide parallization parameters (partitionColumn, lowerBound, upperBound, numPartitions)
* Results
  * As expected, this resulted in a parallel data pull using multiple Spark workers.
  * The tasks were spread over several workers, for example
    * Worker 1: select * from db.schema.table where key >= 0 and key < 1000000
    * Worker 2: select * from db.schema.table where key >= 1000000 and key < 2000000
    * Worker 3: select * from db.schema.table where key >= 2000000 and key < 3000000
  * Unexpected, the test showed that with this particular data pull took as long as the sequential pull for this particular dataset and query.
    * The ordinality of the index column did not allow the load to be spread evenly across partitioned pulls, especially when using the "limit" clause.
    * Thus, even though the tasks were spread over many workers, some tasks were for a small subset of data, while one
      task downloaded nearly all of the data.

## Configuring the benchmarks

* Create the file snowflake-job/snowflake_config.json

```json
{
  "username": "<snowflake_username>",
  "password": "<snowflake_password>",
  "database": "<snowflake_db>",
  "role": "<snowflake_role>",
  "schema": "<snowflake_schema>",
  "url": "<snowflake_instance>.<aws_region>.snowflakecomputing.com:443",
  "warehouse": "<snowflake_warehouse>",
  "table": "<showflake>table"
}
```

* Edit the query in Makefile


## Executing the benchmarks

```
# Start the docker cluster
make spark-start

# Run all of the pull mechanisms
make snowflake all

# While running, View the spark-client Spark UI
: http://localhost:8081

# Check out the results to see the time it took.
tail logs/*
```

