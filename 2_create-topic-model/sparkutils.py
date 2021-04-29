import sys
from typing import Union

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def config_spark(app_name: str, num_cores: int = None) -> SparkSession:
    spark_conf = SparkConf().setAppName(app_name)
    if num_cores is not None:
        spark_conf.setMaster(f"local[{num_cores}]")

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    return spark


def stop_spark_and_exit(spark: Union[SparkSession, SparkContext], exit_code: int = 0):
    try:
        spark.stop()
    finally:
        sys.exit(exit_code)
