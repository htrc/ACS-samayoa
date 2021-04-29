import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta

from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.clustering import LocalLDAModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, expr, arrays_zip, concat_ws
from pyspark.sql.types import ArrayType, StringType

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os


def run_spark(args, spark: SparkSession):
    logger.info(f"Loading topic model from {args.input}...")
    topic_model = LocalLDAModel.load(os.path.join(args.input, 'topicmodel'))

    logger.info("Loading vocab model...")
    vocab_model = CountVectorizerModel.load(os.path.join(args.input, 'vocab'))

    logger.info("Extracting vocabulary...")
    vocab = vocab_model.vocabulary

    topics = topic_model.describeTopics(maxTermsPerTopic=args.num_terms)

    index_map_udf = udf(lambda arr: [vocab[i] for i in arr], ArrayType(StringType()))
    term_weights_udf = udf(lambda arr: [f'{v:.3f}*"{k}"' for k, v in arr], ArrayType(StringType()))

    topics_with_terms = topics.withColumn('topic_terms', index_map_udf(topics.termIndices)).drop('termIndices') \
        .withColumn('terms_and_weights', term_weights_udf(arrays_zip(col('topic_terms'), col('termWeights')))) \
        .withColumn('sum_of_weights', expr('AGGREGATE(termWeights, DOUBLE(0), (acc, x) -> acc + x)')) \
        .drop('termWeights') \
        .withColumn('topic_terms', concat_ws(', ', col('topic_terms'))) \
        .withColumn('terms_and_weights', concat_ws(', ', col('terms_and_weights')))

    if args.output is not None:
        logger.info(f"Saving result to {args.output} ...")
        topics_with_terms.write.csv(os.path.join(args.output, 'topics'))
    else:
        topics_with_terms.show(truncate=False)


def main(args):
    spark = config_spark("ACS Samayoa: List topics", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa: List topics")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-o', '--output', required=False, type=str, metavar='OUTPUT_FILE',
                        help="Write the visualization HTML output to OUTPUT_FILE")
    parser.add_argument('-t', '--num-terms', required=False, type=str, metavar='NUM_TERMS_PER_TOPIC', default=20,
                        help="The number of top terms to display per topic")
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the saved topic model output")

    args = parser.parse_args()

    main(args)
