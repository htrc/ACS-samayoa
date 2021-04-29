import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from typing import Set

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os


def remove_stop_words(page: str, stop_words: Set[str]) -> str:
    return ' '.join(w for w in page.split() if w not in stop_words)


def run_spark(args, spark: SparkSession):
    sc: SparkContext = spark.sparkContext

    stop_words = {}
    if args.stop_words is not None:
        stop_words = set(spark.read.csv(args.stop_words).rdd.map(lambda row: row._c0).collect())

    stop_words_bcast = sc.broadcast(stop_words)

    input_df = spark.read.csv(args.input)

    remove_stopwords_udf = udf(lambda page: remove_stop_words(page, stop_words_bcast.value))
    out_df = input_df.withColumn('_c0', remove_stopwords_udf(col('_c0')))

    out_df.write.csv(os.path.join(args.output, 'cleaned-pages'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("ACS Samayoa: Step 4 - Remove outlier words", args.cores)

    logger.info("Starting...")
    logger.info(f"Spark master: {spark.conf.get('spark.master')}")

    t0 = timer()
    try:
        run_spark(args, spark)
    except Exception:
        logger.exception("Uncaught exception")
        stop_spark_and_exit(spark, exit_code=500)
    else:
        # calculate elapsed execution time
        t1 = timer()
        elapsed = t1 - t0

        logger.info(f"All done in {timedelta(seconds=elapsed)}")
        stop_spark_and_exit(spark)


if __name__ == "__main__":
    logzero.loglevel(logging.INFO)

    parser = argparse.ArgumentParser(description="ACS Samayoa: Step 4 - Remove outlier words")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-s', '--stop-words', required=True, type=str, metavar='STOPWORDS_FILE',
                        help='The file containing the stopwords to be removed')
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the input CSV partitions to process")

    args = parser.parse_args()

    main(args)
