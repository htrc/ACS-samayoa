import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from typing import Set, List, Tuple

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

from argparsetypes import dir_path_type, num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os
import spacy
import pcre


nlp = spacy.load('en')
clean_text_regex = pcre.compile(r'''\b\p{L}+(?:(?:-|_{1,2})\p{L}+)*\b|\b(?:1[89]\d{2}|20[01]\d)\b''')
blacklist_matches_regex = pcre.compile(r'''º|__''')


def preprocess_page(page: str, patterns: List[Tuple[str, pcre.Pattern]], stop_words: Set[str], nounphrases: List[str]) -> str:
    sentences = pcre.split(r'\t', page)
    for i, sent in enumerate(sentences):
        for r, p in patterns:
            sent = p.sub(r, sent)

        sdoc = nlp(sent)
        sent = ' '.join(w for w in (t.lemma_.lower() for t in sdoc) if w not in stop_words and next(blacklist_matches_regex.finditer(w), None) is None)

        sentences[i] = sent

    page = '\t'.join(sentences)

    # connect all noun-phrases
    for np in nounphrases:
        page = pcre.sub(r'\b\Q' + np + r'\E\b', np.replace(' ', '__'), page)

    # remove punctuation and numbers, unless number is 4 digits
    page = ' '.join(w for w in (m.group() for m in clean_text_regex.finditer(page)) if len(w) > 2)

    return page


def run_spark(args, spark: SparkSession):
    sc: SparkContext = spark.sparkContext

    stop_words = {}
    if args.stop_words is not None:
        stop_words = set(spark.read.csv(args.stop_words).rdd.map(lambda row: row._c0).collect())
    stop_words.add('-pron-')

    patterns = []
    if args.patterns is not None:
        with open(args.patterns, 'r', encoding='utf-8') as f:
            patterns = [ (r,pcre.compile(p)) for r,p in (line.rstrip('\n').split('\t') for line in f.readlines()) ]

    noun_phrases = []
    if args.noun_phrases is not None:
        noun_phrases = spark.read.csv(args.noun_phrases).rdd.map(lambda row: row._c0).collect()

    stop_words_bcast = sc.broadcast(stop_words)
    patterns_bcast = sc.broadcast(patterns)
    noun_phrases_bcast = sc.broadcast(noun_phrases)

    preprocess_page_udf = udf(lambda page: preprocess_page(page, patterns_bcast.value, stop_words_bcast.value, noun_phrases_bcast.value), StringType())

    input_df = spark.read.csv(args.input)
    pages_df = input_df.withColumn('_c0', preprocess_page_udf(col('_c0')))

    pages_df.select('_c0', '_c1', '_c2').write.csv(os.path.join(args.output, 'pages'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("ACS Samayoa - Step2: Preprocess pages", args.cores)

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

    parser = argparse.ArgumentParser(description="ACS Samayoa - Step2: Preprocess pages")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-s', '--stop-words', required=False, type=str, metavar='STOPWORDS_FILE',
                        help='The file containing the stopwords to be removed')
    parser.add_argument('-p', '--patterns', required=False, type=str, metavar='PATTERNS_FILE',
                        help='The file containing the patterns to be searched/replaced')
    parser.add_argument('input', type=dir_path_type, metavar='INPUT_DIR',
                        help="The folder containing the input CSV partitions to process")
    parser.add_argument('-n', '--noun-phrases', required=False, type=str, metavar='NOUNPHRASES_FILE',
                        help='The file containing the noun-phrases to fuze')
    args = parser.parse_args()

    main(args)
