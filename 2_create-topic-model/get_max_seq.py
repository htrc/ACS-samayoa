import argparse
import logging
import logzero
from logzero import logger
from timeit import default_timer as timer
from datetime import timedelta
from typing import Tuple, Optional

from pyspark import SparkContext
from pyspark.sql import SparkSession

from argparsetypes import num_cores_type
from sparkutils import config_spark, stop_spark_and_exit

import os
from pairtree import id_encode, id2path
import xml.etree.ElementTree as ET


def id_to_mets(id: str) -> str:
    lib, pid = id.split('.', maxsplit=1)
    enc_pid = id_encode(pid)
    pid_base = id2path(pid)
    return os.path.join(lib, 'pairtree_root', pid_base, enc_pid, f'{enc_pid}.mets.xml')


def mets_page_info(f: str) -> Optional[int]:
    namespaces = {'METS': 'http://www.loc.gov/METS/', 'xlink': 'http://www.w3.org/1999/xlink'}
    try:
        mets = ET.parse(f)
        last_seq_xml = mets.find('.//METS:fileGrp[@USE="ocr"]/METS:file[@MIMETYPE="text/plain"][last()]', namespaces=namespaces)
        last_seq = int(last_seq_xml.attrib['SEQ'])
        
        return last_seq
    except Exception:
        logger.exception(f"Exception processing {f}")
        return None


def run_spark(args, spark: SparkSession):
    sc: SparkContext = spark.sparkContext

    header = ['id', 'last_seq']

    num_partitions = args.num_partitions or sc.defaultParallelism

    ids_rdd = sc.textFile(args.input, minPartitions=num_partitions)
    mets_rdd = ids_rdd.map(lambda id: (id, os.path.join(args.pairtree_path, id_to_mets(id))))
    pageinfo_df = mets_rdd.mapValues(lambda f: mets_page_info(f)) \
        .filter(lambda x: x[1] is not None) \
        .toDF(header)

    with open(os.path.join(args.output, 'header.csv'), 'w') as f:
        f.write(','.join(header) + '\n')
    pageinfo_df.write.csv(os.path.join(args.output, 'pageinfo'))


def main(args):
    os.makedirs(args.output)

    spark = config_spark("Volumes page info", args.cores)

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

    parser = argparse.ArgumentParser(description="Get details about the number of pages of volumes")
    parser.add_argument('-o', '--output', required=True, type=str, metavar='OUTPUT_DIR',
                        help="Write the output to DIR (should not exist, or be empty)")
    parser.add_argument('-p', '--pairtree-path', required=True, type=str, metavar='PAIRTREE_DIR',
                        help="The pairtree root directory")
    parser.add_argument('-c', '--cores', required=False, type=num_cores_type, metavar='NUM_CORES',
                        help="The number of CPU cores to use (if not specified, uses all available cores)")
    parser.add_argument('-n', '--num-partitions', required=False, type=int, metavar='NUM_PARTITIONS',
                        help="The number of partitions to split the input data in, for increased parallelism")
    parser.add_argument('input', type=str, metavar='INPUT_VOLIDS',
                        help="The file containing the volume IDs to process")

    args = parser.parse_args()

    main(args)
