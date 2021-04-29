import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta

from pyspark.sql import SparkSession

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os


def run_spark(args, spark: SparkSession):
    input_df = spark.read.csv(args.input)
    tokens_to_drop_df = input_df.rdd \
        .flatMap(lambda row: ((unique_tok, 1) for unique_tok in set(row._c0.split()))) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] < args.min or x[1] > args.max) \
        .toDF(['token', 'num_pages'])

    tokens_to_drop_df.write.csv(os.path.join(args.output, 'outlier-tokens'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("ACS Samayoa: Step 3 - Find outlier words", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa: Step 3 - Find outlier words")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-m', '--min', required=True, type=int, metavar='MIN_PAGES',
                        help='Remove any tokens that do not occur in at least MIN_PAGES pages')
    parser.add_argument('-x', '--max', required=True, type=int, metavar='MAX_PAGES',
                        help='Remove any tokens that occur in more than MAX_PAGES pages')
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the input CSV partitions to process")

    args = parser.parse_args()

    main(args)
