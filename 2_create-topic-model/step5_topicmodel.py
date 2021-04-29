import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from typing import Set
from pprint import pprint

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, split
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.clustering import LDA

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os


def run_spark(args, spark: SparkSession):
    #sc: SparkContext = spark.sparkContext

    logger.info(f"Reading data from {args.input}...")
    input_df = spark.read.csv(args.input)

    if args.ignore_pages is not None:
        logger.info(f"Reading ignored pages from {args.ignore_pages}...")
        ignore_pages_df = spark.read.csv(args.ignore_pages)
        input_df = input_df.join(ignore_pages_df, (input_df._c1 == ignore_pages_df._c0) & (input_df._c2 == ignore_pages_df._c1), how='left_anti')

    tokenized_df = input_df.where(col('_c0').isNotNull()).withColumn('tokens', split(col('_c0'), ' '))

    count_vectorizer = CountVectorizer(inputCol='tokens', outputCol='features')

    logger.info("Creating dictionary...")
    vocab_model = count_vectorizer.fit(tokenized_df)

    logger.info(f"Saving vocabulary...")
    vocab_model.save(os.path.join(args.output, 'vocab'))

    logger.info("Creating feature vectors...")
    features_df = vocab_model.transform(tokenized_df)

    features_df.show()

    logger.info("Running LDA...")
    lda = LDA(k=args.num_topics, seed=100)
    topic_model = lda.fit(features_df)

    log_likelihood = topic_model.logLikelihood(features_df)
    log_perplexity = topic_model.logPerplexity(features_df)

    logger.info(f"The lower bound on the log likelihood of the entire corpus: {log_likelihood}")
    logger.info(f"The upper bound on perplexity: {log_perplexity}")

    logger.info("Saving LDA model...")
    topic_model.save(os.path.join(args.output, 'topicmodel'))

    logger.info(f"Vocabulary size: {topic_model.vocabSize():,}")
    
    # topics = topic_model.describeTopics(maxTermsPerTopic=20)
    # topics.show(truncate=False)

    result = topic_model.transform(features_df).drop('_c0', 'features')
    result.write.parquet(os.path.join(args.output, 'result'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("ACS Samayoa: Step 5 - Create topic model", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa: Step5 - Create topic model")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-t', '--num-topics', required=True, type=int, metavar='NUM_TOPICS',
                        help='The number of topics to ask LDA to produce')
    parser.add_argument('-i', '--ignore-pages', required=False, type=str, metavar='IGNORE_PAGES_FILE',
                        help="CSV file containing the id,seq representing pages of volumes to be ignored")
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the input CSV dataset to process")

    args = parser.parse_args()

    main(args)
