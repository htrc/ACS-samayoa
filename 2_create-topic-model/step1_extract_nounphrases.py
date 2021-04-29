import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from typing import Iterator, Set

from pyspark import SparkContext
from pyspark.sql import SparkSession

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os
import re
import spacy


nlp = spacy.load('en')


def get_lemmatized_np(sent: str, stop_words: Set[str]) -> Iterator[str]:
    doc = nlp(sent)
    ncl = (nc.lemma_ for nc in doc.noun_chunks)
    cleaned_ncl = ([w for w in nc.split() if w.lower() not in stop_words] for nc in ncl)
    return (' '.join(ncs) for ncs in cleaned_ncl if len(ncs) > 1 and all(t.isalpha() for t in ncs))


def extract_nounphrases(page: str, stop_words: Set[str]) -> Iterator[str]:
    for sent in re.split(r'\t+', page):
        for np in get_lemmatized_np(sent, stop_words):
            yield np


def run_spark(args, spark: SparkSession):
    sc: SparkContext = spark.sparkContext

    stop_words = {}
    if args.stop_words is not None:
        stop_words = set(spark.read.csv(args.stop_words).rdd.map(lambda row: row._c0).collect())
    stop_words.add('-pron-')

    stop_words_bcast = sc.broadcast(stop_words)

    pages_rdd = spark.read.csv(args.input).select('_c0').rdd.map(lambda row: row._c0)
    sorted_nounphrase_counts_df = pages_rdd \
        .flatMap(lambda page: list(extract_nounphrases(page, stop_words_bcast.value))) \
        .map(lambda np: (np.lower(), 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .filter(lambda x: x[1] > 10) \
        .sortBy(lambda x: -x[1]) \
        .toDF(['noun_phrase', 'count'])

    sorted_nounphrase_counts_df.write.csv(os.path.join(args.output, 'nps'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("ACS Samayoa - Step1: Extract noun-phrases", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa - Step1: Extract noun-phrases")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-s', '--stop-words', required=False, type=str, metavar='STOPWORDS_FILE',
                        help='The file containing the stopwords to be removed')
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the input CSV partitions to process")

    args = parser.parse_args()

    main(args)
