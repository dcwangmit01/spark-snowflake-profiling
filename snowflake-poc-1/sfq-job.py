#!/usr/bin/env python3
# based on: https://docs.snowflake.com/en/user-guide/spark-connector-use.html#sample-python-script
'''sfq-job

Usage:
  sfq-job.py [-d] [-p] [--sleep=SECONDS] OUTPUT_FILE SQL_QUERY
  sfq-job.py (-h | --help)

Examples:
  spark-submit sfq-job.py output.tsv 'SELECT * FROM db.schema.table limit 10'

Arguments:
  OUTPUT_FILE    Tab-separated values file. Folder is created if it does not yet exist.
  SQL_QUERY      SQL query to execute against Snowflake.

Options:
  -h --help          Show this screen
  -d --debug         Debug output
  -p --parallel      Do a Parallel SQL Pull [default: false]
  --sleep=<seconds>  Sleep after execution to keep Spark UI available [default: 0]
'''

import json
import os
import time

from docopt import docopt
from pathlib import Path
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def dict2json(d):
    return json.dumps(d, sort_keys=True, indent=2)


if __name__ == '__main__':
    args = docopt(__doc__)
    args['--debug'] and print('==> Debug args.')
    args['--debug'] and print(dict2json(args))

    print('==> Prepare to query Snowflake via Spark.')
    master = os.getenv('SPARK_MASTER_URL')
    app = 'sfq-job'

    sc = SparkContext(master, app)
    spark = SQLContext(sc)
    spark_conf = SparkConf().setMaster(master).setAppName(app)

    # TODO: We might need to set this in the future, once we're allowed to use AWS S3.
    # sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId', '<YOUR_AWS_KEY>')
    # sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey', '<YOUR_AWS_SECRET>')

    sfOptions = {
        'sfURL': os.getenv('SNOWFLAKE_URL'),
        'sfUser': os.getenv('SNOWFLAKE_USERNAME'),
        'sfPassword': os.getenv('SNOWFLAKE_PASSWORD'),
        'sfDatabase': os.getenv('SNOWFLAKE_DATABASE'),
        'sfSchema': os.getenv('SNOWFLAKE_SCHEMA'),
        'sfRole': os.getenv('SNOWFLAKE_ROLE'),
        'sfWarehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    }

    args['--debug'] and print(dict2json(sfOptions))

    SNOWFLAKE_SOURCE_NAME = 'net.snowflake.spark.snowflake'

    print('==> Execute Snowflake query and write output file.')
    output_dir = os.path.dirname(Path(args['OUTPUT_FILE']))
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    df = None
    if args['--parallel']:
        df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option('query',  args['SQL_QUERY']) \
            .load()
    else:
        df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .option('query',  args['SQL_QUERY']) \
            .load()

    df.show()
    df.write.option('delimiter', '\t').csv(args['OUTPUT_FILE'])
    print('==> Wrote file to local storage: {"path": "%s"}' % args['OUTPUT_FILE'])

    # Sleep after execution to keep Spark UI Open
    time.sleep(int(args['--sleep']))
