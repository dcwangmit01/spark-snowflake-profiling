#!/usr/bin/env python
"""
Example
"""

#####################################################################
# Load the configuration file
config = None
import yaml
with open("config.yaml", 'r') as stream:
    config = yaml.safe_load(stream)
print(config)

#####################################################################
# Process each resource element in the configuration file

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType

spark = SparkSession.builder.master("local[*]") \
    .config("spark.sql.caseSensitive", True) \
    .appName("ProcessData") \
    .getOrCreate()

# spark.sql('CREATE DATABASE IF NOT EXISTS aws')
# spark.sql('USE aws')

table_names = []
for resource in config['config']['resources']:
    # path: out/<aws_profile>/<aws_region>/<cloud-custodian-resource-name>-all/resources.json
    dashed_name = resource.replace('.', '-') + '-all'
    table_name = resource.replace('.', '_').replace('-', '_').replace('aws_', '')

    table_names.append(table_name)
    path = "out/*/*/{}/resources.json".format(dashed_name)
    df = spark.read.option("multiline", "true").json(path)
    df.printSchema()
    df.createOrReplaceTempView(table_name)
    query = "select * from {}".format(table_name)

    spark.sql(query).show()

spark.sql('show tables').show()

import ipdb; ipdb.set_trace()



