import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta

from pyspark.ml.feature import CountVectorizerModel
from pyspark.ml.clustering import LocalLDAModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, size

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import numpy as np
import os
import pyLDAvis


def pyldavis_generate(topic_model, result, vocab):
    logger.info("[1/4] Generating the term frequency matrix...")
    term_frequency_df = result.select(explode('tokens').alias('terms')).groupBy('terms').count()
    term_frequency = {row[0]: row[1] for row in term_frequency_df.collect()}
    term_frequency = np.array([term_frequency[w] for w in vocab])

    logger.info("[2/4] Computing the document lengths...")
    doc_lengths = np.array(result.withColumn('doc_len', size(result.tokens)).select(col('doc_len')).rdd.map(lambda row: row[0]).collect())
    
    logger.info("[3/4] Retrieving the document -> topic distribution...")
    doc_topic_dists = np.array([row.toArray() for row in result.select(result.topicDistribution).toPandas()['topicDistribution']])
    
    logger.info("[4/4] Retrieving the term -> topic distribution...")
    topic_term_dists = np.array([row for row in topic_model.describeTopics(maxTermsPerTopic=topic_model.vocabSize()).select(col('termWeights')).toPandas()['termWeights']])
    
    # doc_topic_dists = np.array(result.select(result.topicDistribution).rdd.map(lambda row: row[0].toArray()).collect())
    # topic_term_dists = np.array(topic_model.describeTopics(maxTermsPerTopic=len(vocab)).select(col('termWeights')).rdd.map(lambda row: row[0]).collect())

    return {
        'topic_term_dists': topic_term_dists,
        'doc_topic_dists': doc_topic_dists,
        'doc_lengths': doc_lengths,
        'vocab': vocab,
        'term_frequency': term_frequency
    }


def run_spark(args, spark: SparkSession):
    logger.info(f"Loading topic model from {args.input}...")
    topic_model = LocalLDAModel.load(os.path.join(args.input, 'topicmodel'))

    logger.info("Loading vocab model...")
    vocab_model = CountVectorizerModel.load(os.path.join(args.input, 'vocab'))

    logger.info("Extracting vocabulary...")
    vocab = vocab_model.vocabulary

    logger.info("Loading result...")
    result = spark.read.parquet(os.path.join(args.input, 'result'))

    logger.info("Generating data for pyLDAvis...")
    pyldavis_data = pyldavis_generate(topic_model, result, vocab)

    logger.info("Preparing pyLDAvis data...")
    vis = pyLDAvis.prepare(**pyldavis_data)

    if args.output is not None:
        logger.info(f"Saving result to {args.output} ...")
        pyLDAvis.save_html(vis, args.output)

    if args.port is not None:
        logger.info(f"Serving result on port {args.port}...")
        pyLDAvis.show(vis, port=args.port, open_browser=False)

    # feature_corpus_counts_df = result.select(explode('tokens').alias('col')).groupBy('col').count()

    # index_map_udf = udf(lambda arr: [vocab[i] for i in arr], ArrayType(StringType()))
    # topics_with_terms = topics.withColumn('topic_terms', index_map_udf(topics.termIndices)).drop('termIndices')

    # log_likelihood = topic_model.logLikelihood(result)
    # log_perplexity = topic_model.logPerplexity(result)

    # logger.info(f"The lower bound on the log likelihood of the entire corpus: {log_likelihood}")
    # logger.info(f"The upper bound on perplexity: {log_perplexity}")


def main(args):
    if args.output is None and args.port is None:
        raise argparse.ArgumentError(argument=None, message="One of 'output' or 'port' must be specified!")

    spark = config_spark("ACS Samayoa: Step6 - Process and visualize the topic model", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa: Step6 - Process and visualize the topic model")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-p', '--port', required=False, type=int, metavar='PORT',
                        help="The port where to serve the visualization from")
    parser.add_argument('-o', '--output', required=False, type=str, metavar='OUTPUT_FILE',
                        help="Write the visualization HTML output to OUTPUT_FILE")
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the saved topic model output")

    args = parser.parse_args()

    main(args)
