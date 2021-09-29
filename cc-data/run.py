#!/usr/bin/env python
"""Process Cloud Custodian Data

Usage:
  run.py <config_path> <data_path>

Options:
  -h --help     Show this screen.

"""
from docopt import docopt

import time
import threading

import yaml

from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType, DoubleType  # noqa;
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException


class CloudCustodianSpark(object):
    def __init__(self, args):
        self.args = args
        self.conf = self.load_config(args['<config_path>'])
        self.data_path = args['<data_path>']

        self.spark = SparkSession.builder \
            .config("spark.sql.caseSensitive", True) \
            .appName("cc-data") \
            .getOrCreate()

        self.dfs = {}

    @staticmethod
    def load_config(config_path):
        config = None
        with open(config_path, 'r') as stream:
            config = yaml.safe_load(stream)
        return config

    def run(self):
        parallel_jobs = True
        if parallel_jobs:
            threads = []
            for resource in self.conf['config']['resources']:
                t = threading.Thread(target=self.create_resource_df, args=(resource, ))
                threads.append(t)
            t = threading.Thread(target=self.create_master_table)
            threads.append(t)
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        else:
            for resource in self.conf['config']['resources']:
                self.create_resource_df(resource)
                self.create_master_table()

        self.spark.sql('show tables').show()
        self.spark.sql('select cc_resource, cc_tagkey, cc_tagval from all_dtags' +
                       ' where cc_tagval like "%davwang4%"'). \
            show(9999, truncate=False)

        # Sleep so we can access the UI
        time.sleep(6000)

    def create_resource_df(self, resource_name):
        print("Starting {}".format(resource_name))
        directory_name = resource_name.replace('.', '-') + '-all'
        table_name = resource_name.replace('.', '_').replace('-', '_').replace('aws_', '')
        denormalized_table_name = table_name + "_dtags"

        # path: '.../<aws_profile>/<aws_region>/<cloud-custodian-resource-name>-all/resources.json'
        path = "{}/*/*/{}/resources.json".format(self.data_path, directory_name)

        df = self.spark.read.option("multiline", "true").json(path) \
            .withColumn('cc_resource', F.lit(resource_name)) \
            .withColumn("cc_profile", F.element_at(F.split(F.input_file_name(), '/'), -4)) \
            .withColumn("cc_region", F.element_at(F.split(F.input_file_name(), '/'), -3))
        df.createOrReplaceTempView(table_name)
        self.dfs[table_name] = df
        df.printSchema()

        if 'Tags' in df.columns:
            # If Tags are all null, then we won't be able to parse cc_tagkey and cc_tagval.
            try:
                tags_df = df.withColumn('_tmpTag', F.explode('Tags')) \
                    .select('*', '_tmpTag') \
                    .withColumn('cc_tagkey', F.col('_tmpTag.Key')) \
                    .withColumn('cc_tagval', F.col('_tmpTag.Value')) \
                    .drop(F.col('_tmpTag'))
                tags_df.createOrReplaceTempView(denormalized_table_name)
                self.dfs[denormalized_table_name] = tags_df
            except (AnalysisException):
                pass

    def create_master_table(self):

        table_name = 'all'
        denormalized_table_name = table_name + "_dtags"

        # path: out/<aws_profile>/<aws_region>/<cloud-custodian-resource-name>-all/resources.json
        path = "{}/*/*/*/resources.json".format(self.data_path)
        df = self.spark.read.option("multiline", "true").json(path) \
            .withColumn('cc_resource', F.regexp_replace(
                F.element_at(F.split(F.input_file_name(), '/'), -2), '-all', '')) \
            .withColumn("cc_profile", F.element_at(F.split(F.input_file_name(), '/'), -4)) \
            .withColumn("cc_region", F.element_at(F.split(F.input_file_name(), '/'), -3))
        df.createOrReplaceTempView(table_name)
        self.dfs[table_name] = df

        # Create Master Table with Denormalized Tags
        tags_df = df.withColumn('_tmpTag', F.explode('Tags')).select('*', '_tmpTag').withColumn(
            'cc_tagkey', F.col('_tmpTag.Key')).withColumn('cc_tagval', F.col('_tmpTag.Value')).drop(F.col('_tmpTag'))
        tags_df.createOrReplaceTempView(denormalized_table_name)
        self.dfs[denormalized_table_name] = tags_df


#####################################################################
# Process each resource element in the configuration file

if __name__ == '__main__':
    args = docopt(__doc__, version='0.1.0')
    ccs = CloudCustodianSpark(args)
    ccs.run()
""" Notes
    # import ipdb; ipdb.set_trace()

    # dfs['all'].write.format(
    #     "org.elasticsearch.spark.sql"
    # ).option(
    #     "es.resource", '%s/%s' % (config['config']['elasticsearch']['index'], \
    #          config['config']['elasticsearch']['doc_type'])
    # ).option(
    #     "es.nodes", config['config']['elasticsearch']['host']
    # ).option(
    #     "es.port", config['config']['elasticsearch']['port']
    # ).save()


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

"""
