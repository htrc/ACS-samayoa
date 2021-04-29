import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from pprint import pprint

from gensim import corpora
from gensim.models import LdaModel, CoherenceModel

import os
import sys
import csv


def main(args):
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

    os.makedirs(args.output)
    logger.info(f"Reading data from {args.input}...")

    documents = []
    with open(args.input, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        documents = [row[0].split(' ') for row in reader]

    logger.info("Creating dictionary...")
    dictionary = corpora.Dictionary(documents)

    logger.info("Creating the bag-of-words...")
    corpus = [dictionary.doc2bow(doc) for doc in documents]

    logger.info("Training the topic model...")

    topic_model = LdaModel(
        corpus,
        id2word=dictionary,
        num_topics=args.num_topics,
        per_word_topics=True,
        random_state=100,
        chunksize=10000,
        update_every=1,
        passes=10,
        distributed=True)

    logger.info("Saving the topic model...")
    topic_model.save(os.path.join(args.output, f'model_{args.num_topics}.lda'))

    logger.info("Constructing the coherence model...")
    coherence_model = CoherenceModel(model=topic_model, texts=documents, dictionary=dictionary, coherence='c_v')

    logger.info("Saving the coherence model...")
    coherence_model.save(os.path.join(args.output, f'coherence_{args.num_topics}.lda'))

    logger.info("Calculating perplexity and coherence scores...")

    perplexity_score = topic_model.log_perplexity(corpus)
    coherence_score = coherence_model.get_coherence()

    logger.info(f"Perplexity score: {perplexity_score}")
    logger.info(f"Coherence score: {coherence_score}")

    pprint(topic_model.print_topics())


def _main(args):
    logger.info("Starting...")

    error = False
    t0 = timer()
    try:
        main(args)
    except Exception:
        logger.exception("Uncaught exception")
        error = True
    finally:
        # calculate elapsed execution time
        t1 = timer()
        elapsed = t1 - t0

        logger.info(f"All done in {timedelta(seconds=elapsed)}")

        if error:
            sys.exit(1)


if __name__ == "__main__":
    logzero.loglevel(logging.INFO)

    parser = argparse.ArgumentParser(description="ACS Samayoa: Step5 - Create topic model")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-t', '--num-topics', required=True, type=int, metavar='NUM_TOPICS',
                        help='The number of topics to ask LDA to produce')
    parser.add_argument('input', type=str, metavar='INPUT_FILE',
                        help="The file containing the input CSV dataset to process")

    args = parser.parse_args()

    _main(args)
