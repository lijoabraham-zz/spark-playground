"""
spark.py
~~~~~~~~
Module containing helper function for use with Apache Spark
"""

import json
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from dependencies import logging
from os import path

class SparkConnection(object):

    def __init__(self, params):
        self.app_name = params.get('app_name')
        self.files = params.get('files')
        # get spark app details with which to prefix all messages
        self.start_spark(app_name=self.app_name, files=self.files)


    def start_spark(self, app_name='my_spark_app', master='local[*]', jar_packages=[], files=[], spark_config={}):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .buider
            .master(master)
            .appName(app_name))
        # create Spark JAR packages string
        spark_jars_packages = ','.join(list(jar_packages))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other configs params
        for key, val in spark_config.items():
            spark_builder.config(key, val)

        # create session and retrieve Spark logger object
        spark_sess = spark_builder.getOrCreate()
        spark_logger = logging.Log4j(spark_sess)

        # get configs file if sent to cluster with --files
        spark_files_dir = SparkFiles.getRootDirectory()
        config_files = [filename
                        for filename in list(dir(spark_files_dir))
                        if filename.endswith('json')]

        if config_files:
            path_to_config_file = path.join(spark_files_dir, config_files[0])
            with open(path_to_config_file, 'r') as config_file:
                config_dict = json.load(config_file)
            spark_logger.warn('loaded configs from ' + config_files[0])
        else:
            spark_logger.warn('no configs file found')
            config_dict = None

        self.spark = spark_sess
        self.log = spark_logger
        self.config_data = config_dict

        return True
