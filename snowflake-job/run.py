#!/usr/bin/env python3
'''snowflake-job.py

Usage:
  snowflake-job.py (-d | --debug) (--sleep=SECONDS) \
    (  --snowflake-spark-sequential \
     | --snowflake-spark-parallel   \
     | --snowflake-jdbc-sequential  \
     | --snowflake-jdbc-parallel )  \
    OUTPUT_DIR SQL_QUERY
  snowflake-job.py (-h | --help)

Examples:
  snowflake-job.py output_dir 'SELECT * FROM db.schema.table LIMIT 10'

Arguments:
  OUTPUT_DIR     Parquet output directory.  Folder is created if it does not yet exist.
  SQL_QUERY      SQL query to execute against Snowflake.

Options:
  -d --debug                    Debug output
  --sleep=<seconds>             Sleep after execution to keep Spark UI available [default: 0]
  --snowflake-spark-sequential  Use Snowflake Spark Driver to do Sequential Pull
  --snowflake-spark-parallel    Use Snowflake Spark Driver to do Parallel Pull
  --snowflake-jdbc-sequential   Use Snowflake JDBC Driver to do Sequential Pull
  --snowflake-jdbc-parallel     Use Snowflake JDBC Driver to do Parallel Pull
  -h --help                     Show this screen
'''

import json
import logging
import os
import time

from docopt import docopt
from pathlib import Path
from pyspark import SparkContext
from pyspark.sql import SQLContext

logger = logging.getLogger('snowflake-job')
logger.setLevel(logging.DEBUG)


def dict2json(d):
    return json.dumps(d, sort_keys=True, indent=2)


class SnowflakeSparkBenchmark(object):

    SNOWFLAKE_SPARK_SOURCE_NAME = 'net.snowflake.spark.snowflake'
    SNOWFLAKE_JDBC_SOURCE_NAME = 'net.snowflake.client.jdbc.SnowflakeDriver'

    # TODO: Hardcoded Vars to be removed
    sql_partition_column = "C_CUSTKEY"

    def __init__(self, args):
        self.args = args
        self.env = {k: os.environ.get(k) for k, v in os.environ.items()}

        # Instance Spark Variables
        self.spark_app = 'snowflake-job'  # Define the app name
        for i in self.args:
            if i.startswith('--snowflake-') and self.args[i]:
                self.spark_app += i

        self.spark_master = os.getenv('SPARK_MASTER_URL')

        # Instance Snowflake Variables
        self.snowflake_spark_driver_options = {
            'sfURL': os.getenv('SNOWFLAKE_URL'),
            'sfUser': os.getenv('SNOWFLAKE_USERNAME'),
            'sfPassword': os.getenv('SNOWFLAKE_PASSWORD'),
            'sfDatabase': os.getenv('SNOWFLAKE_DATABASE'),
            'sfSchema': os.getenv('SNOWFLAKE_SCHEMA'),
            'sfRole': os.getenv('SNOWFLAKE_ROLE'),
            'sfWarehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
        }

        self.snowflake_jdbc_driver_options = {
            "driver": self.SNOWFLAKE_JDBC_SOURCE_NAME,
            "user": self.snowflake_spark_driver_options['sfUser'],
            "password": self.snowflake_spark_driver_options['sfPassword'],
            "db": self.snowflake_spark_driver_options['sfDatabase'],
            "role": self.snowflake_spark_driver_options['sfRole'],
            "schema": self.snowflake_spark_driver_options['sfSchema'],
            "warehouse": self.snowflake_spark_driver_options['sfWarehouse'],
        }
        self.snowflake_jdbc_url = 'jdbc:snowflake://{}/'.format(self.snowflake_spark_driver_options['sfURL'])

        # Debug Output
        if args['--debug']:
            print("ARGS = ", dict2json(self.args))
            print("ENV = ", dict2json(self.env))
            print("SNOWFLAKE_SPARK_DRIVER_OPTIONS = ", dict2json(self.snowflake_spark_driver_options))
            print("SNOWFLAKE_JDBC_DRIVER_OPTIONS = ", dict2json(self.snowflake_jdbc_driver_options))

        # Query Variables
        self.sql_query = args['SQL_QUERY']
        self.sql_num_partitions = int(self.env['SPARK_WORKER_REPLICAS']) * int(self.env['SPARK_WORKER_CORES'])

        sc = SparkContext(self.spark_master, self.spark_app)
        self.spark = SQLContext(sc)

    def run(self):

        df = None
        if args['--snowflake-spark-sequential']:
            df = self.snowflake_spark_sequential()
        elif args['--snowflake-spark-parallel']:
            df = self.snowflake_spark_parallel()
        elif args['--snowflake-jdbc-sequential']:
            df = self.snowflake_jdbc_sequential()
        elif args['--snowflake-jdbc-parallel']:
            df = self.snowflake_jdbc_parallel()

        # Create output directory
        output_dir = os.path.dirname(Path(args['OUTPUT_DIR']))
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Execute the query (df is lazy-evaluated) and write to parquet
        df.write.parquet(args['OUTPUT_DIR'])

        # Sleep after execution to keep Spark UI Open
        time.sleep(int(args['--sleep']))

    def snowflake_spark_sequential(self):
        # This seems to pull and write in parallel anyway
        #   Even when run without the partition options.
        df = self.spark.read.format(self.SNOWFLAKE_SPARK_SOURCE_NAME) \
            .options(**self.snowflake_spark_driver_options) \
            .option('query',  self.sql_query) \
            .load()
        return df

    def snowflake_spark_parallel(self):
        # Parallel options seem to be ignored
        # Seems to have same execution characteristics as snowflake_spark_sequential

        # Querying partition column lowerbound
        lowerbound_df = self.spark.read.format(self.SNOWFLAKE_SPARK_SOURCE_NAME) \
            .options(**self.snowflake_spark_driver_options) \
            .option("query",
                    f" SELECT min({self.sql_partition_column}) as min_lowerbound " +
                    f"FROM ({self.sql_query}) AS inner_query")  \
            .load()
        min_lowerbound = lowerbound_df.collect()[0]['MIN_LOWERBOUND']
        logger.info(f"MIN_LOWERBOUND is {min_lowerbound}")

        # Querying partition column upperbound
        upperbound_df = self.spark.read.format(self.SNOWFLAKE_SPARK_SOURCE_NAME) \
            .options(**self.snowflake_spark_driver_options) \
            .option("query",
                    f" SELECT max({self.sql_partition_column}) as max_upperbound " +
                    f"FROM ({self.sql_query}) AS inner_query") \
            .load()
        max_upperbound = upperbound_df.collect()[0]['MAX_UPPERBOUND']
        logger.info(f"MAX_UPPERBOUND is {max_upperbound}")

        df = self.spark.read.format(self.SNOWFLAKE_SPARK_SOURCE_NAME) \
            .options(**self.snowflake_spark_driver_options) \
            .option('query',  self.sql_query) \
            .option("partitionColumn", self.sql_partition_column) \
            .option("lowerBound", min_lowerbound) \
            .option("upperBound", max_upperbound) \
            .option("numPartitions", self.sql_num_partitions) \
            .load()

        return df

    def snowflake_jdbc_sequential(self):
        # https://docs.databricks.com/data/data-sources/sql-databases.html#python-example
        # https://docs.snowflake.com/en/user-guide/jdbc-configure.html#
        #
        # Spark SQL does not work and always errors out with:
        #   pyspark.sql.utils.AnalysisException: net.snowflake.client.jdbc.SnowflakeDriver
        #     is not a valid Spark SQL Data Source.
        # df = self.spark.read.format(SNOWFLAKE_JDBC_SOURCE_NAME) \
        #     .options(**self.snowflake_jdbc_driver_options) \
        #     .option("url", self.snowflake_jdbc_url) \
        #     .option('query',  self.sql_query) \
        #     .load()
        #
        # However, the following syntax works:
        df = self.spark.read.jdbc(url=self.snowflake_jdbc_url,
                                  table="({})".format(self.sql_query),
                                  properties=self.snowflake_jdbc_driver_options)

        return df

    def snowflake_jdbc_parallel(self):
        # https://docs.databricks.com/data/data-sources/sql-databases.html#python-example
        # https://docs.snowflake.com/en/user-guide/jdbc-configure.html#

        # TODO: Wasn't able to query lowerbound and upper bound similar to snowflake_spark_parallel
        #   This, this is hard-coded
        min_lowerbound = 0
        max_upperbound = 5000000

        parallel_options = self.snowflake_jdbc_driver_options.copy()
        parallel_options["partitionColumn"] = self.sql_partition_column
        parallel_options["lowerBound"] = str(min_lowerbound)
        parallel_options["upperBound"] = str(max_upperbound)
        parallel_options["numPartitions"] = str(self.sql_num_partitions)

        # works
        df = self.spark.read.jdbc(url=self.snowflake_jdbc_url,
                                  table="({})".format(self.sql_query),
                                  properties=parallel_options)
        return df


if __name__ == '__main__':
    args = docopt(__doc__, version='0.1.0')
    s = SnowflakeSparkBenchmark(args)
    s.run()
