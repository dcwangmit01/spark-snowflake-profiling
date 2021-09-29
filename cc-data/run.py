#!/usr/bin/env python
"""Process Cloud Custodian Data

Usage:
  run.py <config_path> <data_path>

Options:
  -h --help     Show this screen.

"""
from docopt import docopt

import sys
import time

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

def run(args):

    #####################################################################
    # Load the configuration file
    config = None
    with open(args['<config_path>'], 'r') as stream:
        config = yaml.safe_load(stream)

    data_path = args['<data_path>']

    #####################################################################
    # Process each resource element in the configuration file

    spark = SparkSession.builder \
        .config("spark.sql.caseSensitive", True) \
        .appName("ProcessData") \
        .getOrCreate()

    dfs = {}

    # Create Individual Tables
    for resource in config['config']['resources']:

        directory_name = resource.replace('.', '-') + '-all'
        table_name = resource.replace('.', '_').replace('-', '_').replace('aws_', '')
        denormalized_table_name = table_name+"_dtags"

        # path: '<aws_profile>/<aws_region>/<cloud-custodian-resource-name>-all/resources.json'
        path = "{}/*/*/{}/resources.json".format(data_path, directory_name)

        df = spark.read.option("multiline", "true").json(path) \
            .withColumn('cc_resource', F.lit(resource)) \
            .withColumn("cc_profile",F.element_at(F.split(F.input_file_name(), '/'), -4)) \
            .withColumn("cc_region",F.element_at(F.split(F.input_file_name(), '/'), -3))
        df.createOrReplaceTempView(table_name)
        dfs[table_name] = df

        # df.write.format(
        #     "org.elasticsearch.spark.sql"
        # ).option(
        #     "es.resource", '%s' % (config['config']['elasticsearch']['index'])
        # ).option(
        #     "es.nodes", config['config']['elasticsearch']['host']
        # ).option(
        #     "es.port", config['config']['elasticsearch']['port']
        # ).save()
        # import ipdb; ipdb.set_trace()

        if 'Tags' in df.columns:
            # If Tags are all null, then we won't be able to parse cc_tagkey and cc_tagval.
            try:
                df = df.withColumn('_tmpTag', F.explode('Tags')).select('*', '_tmpTag').withColumn('cc_tagkey', F.col('_tmpTag.Key')).withColumn('cc_tagval', F.col('_tmpTag.Value')).drop(F.col('_tmpTag'))
            except (AnalysisException):
                pass

            df.createOrReplaceTempView(denormalized_table_name)
            dfs[denormalized_table_name] = df

            df.printSchema()

    # Create Master Tables
    if False:
        table_name = 'all'
        denormalized_table_name = table_name+"_dtags"

        # path: out/<aws_profile>/<aws_region>/<cloud-custodian-resource-name>-all/resources.json
        path = "{}/*/*/*/resources.json".format(data_path)
        df = spark.read.option("multiline", "true").json(path) \
            .withColumn('cc_resource', F.regexp_replace(F.element_at(F.split(F.input_file_name(), '/'), -2), '-all', '')) \
            .withColumn("cc_profile",F.element_at(F.split(F.input_file_name(), '/'), -4)) \
            .withColumn("cc_region",F.element_at(F.split(F.input_file_name(), '/'), -3))
        df.createOrReplaceTempView(table_name)
        dfs[table_name] = df

        # Create Master Table with Denormalized Tags
        df = df.withColumn('_tmpTag', F.explode('Tags')).select('*', '_tmpTag').withColumn('cc_tagkey', F.col('_tmpTag.Key')).withColumn('cc_tagval', F.col('_tmpTag.Value')).drop(F.col('_tmpTag'))
        df.createOrReplaceTempView(denormalized_table_name)
        dfs[denormalized_table_name] = df


    time.sleep(6000)

    spark.sql('show tables').show()

    spark.sql('select cc_resource, cc_tagkey, cc_tagval from all_dtags where cc_tagval like "%davwang4%"').show(9999, truncate=False)


    # import ipdb; ipdb.set_trace()

    # dfs['all'].write.format(
    #     "org.elasticsearch.spark.sql"
    # ).option(
    #     "es.resource", '%s/%s' % (config['config']['elasticsearch']['index'], config['config']['elasticsearch']['doc_type'])
    # ).option(
    #     "es.nodes", config['config']['elasticsearch']['host']
    # ).option(
    #     "es.port", config['config']['elasticsearch']['port']
    # ).save()


if __name__ == '__main__':
    arguments = docopt(__doc__, version='0.1.0')
    run(arguments)
