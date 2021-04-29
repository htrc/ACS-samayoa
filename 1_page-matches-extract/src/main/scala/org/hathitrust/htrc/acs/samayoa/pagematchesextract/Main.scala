package org.hathitrust.htrc.acs.samayoa.pagematchesextract

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.hathitrust.htrc.data.{HtrcVolume, HtrcVolumeId}
import Helper._
import org.hathitrust.htrc.tools.scala.io.IOUtils.using
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._

import java.io.PrintWriter
import scala.collection.immutable.TreeMap
import scala.io.{Codec, Source, StdIn}

/**
  * @author Boris Capitanu
  */

object Main {
  val appName = "page-matches-extract"

  def stopSparkAndExit(sc: SparkContext, exitCode: Int = 0): Unit = {
    try {
      sc.stop()
    }
    finally {
      System.exit(exitCode)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val numPartitions = conf.numPartitions.toOption
    val numCores = conf.numCores.map(_.toString).getOrElse("*")
    val pairtreeRootPath = conf.pairtreeRootPath().toString
    val keywordsPath = conf.keywordsPath()
    val outputPath = conf.outputPath().toString
    val htids = conf.htids.toOption match {
      case Some(file) => using(Source.fromFile(file))(_.getLines().toList)
      case None => Iterator.continually(StdIn.readLine()).takeWhile(_ != null).toSeq
    }

    // set up logging destination
    conf.sparkLog.toOption match {
      case Some(logFile) => System.setProperty("spark.logFile", logFile)
      case None =>
    }
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    val sparkConf = new SparkConf()
    sparkConf.setAppName(appName)
    sparkConf.setIfMissing("spark.master", s"local[$numCores]")
    val sparkMaster = sparkConf.get("spark.master")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    try {
      logger.info("Starting...")
      logger.info(s"Spark master: $sparkMaster")

      val t0 = Timer.nanoClock()

      // load the keywords and keyword regex patterns from the input file
      val searchKeywords = using(Source.fromFile(keywordsPath)(Codec.UTF8)) { f =>
        f.getLines()
          .map(_.split("\t") match {
            case Array(name, pat) => name -> pat
          })
          .toList
      }

      val keywords = searchKeywords.map(_._1)
      val pageTextField = StructField("text", StringType, nullable = false)
      val idField = StructField("volid", StringType, nullable = false)
      val seqField = StructField("seq", StringType, nullable = false)
      val kwFields = keywords.map(StructField(_, IntegerType, nullable = false))
      val schema = StructType(Seq(pageTextField, idField, seqField) ++ kwFields)
      logger.info(f"Schema has ${schema.fields.length}%,d fields")

      val searchPatternsBcast = sc.broadcast(searchKeywords.map(_._2))

      conf.outputPath().mkdirs()

      val idsRDD = numPartitions match {
        case Some(n) => sc.parallelize(htids, n) // split input into n partitions
        case None => sc.parallelize(htids) // use default number of partitions
      }

      // load the HTRC volumes from pairtree (and save any errors)
      val volumeErrAcc = new ErrorAccumulator[String, String](identity)(sc)
      val volumesRDD = idsRDD.tryMap { id =>
        val pairtreeVolume =
          HtrcVolumeId
            .parseUnclean(id)
            .map(_.toPairtreeDoc(pairtreeRootPath))
            .get

        HtrcVolume.from(pairtreeVolume)(Codec.UTF8).get
      }(volumeErrAcc)

      val errorsMatches = new ErrorAccumulator[HtrcVolume, String](_.volumeId.uncleanId)(sc)
      val pageMatchesRDD = volumesRDD.tryFlatMap { vol =>
        val searchPatterns = searchPatternsBcast.value.map(_.r)
        val pagesTextWithOccurrences = extractFullPageSentences(vol).iterator
          .map {
            case (seq, sentences) =>
              val text = sentences.iterator.map(_.replaceAll("\n|\r|\t", " ")).mkString("\t")
              val occurrences = countOccurrences(searchPatterns, text)
              //text = text.replaceAll("""[\n+=$^<>~`]+|[^\P{P}-']+|\b(?:\d{1,3}|\d{5,})\b|(?<!\p{L})(?:-+|')(?!\p{L})""", " ")
              (vol.volumeId.uncleanId, seq, text, occurrences)
          }
          .filter { case (_, _, _, occurrences) => occurrences.exists(_ > 0) }
          .toList

        pagesTextWithOccurrences
      }(errorsMatches)

      val rows = pageMatchesRDD.map { case (volid, seq, text, occurrences) =>
        val rowData = Seq(text, volid, seq) ++ occurrences
        Row.fromSeq(rowData)
      }

      val rowsDF = spark.createDataFrame(rows, schema)

      rowsDF.write
        .option("header", "false")
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .csv(outputPath + "/extracted")

      using(new PrintWriter(outputPath + "/header.tsv"))(_.println(schema.fieldNames.mkString(",")))

      if (volumeErrAcc.nonEmpty || errorsMatches.nonEmpty) {
        logger.info("Writing error report(s)...")
        if (volumeErrAcc.nonEmpty)
          volumeErrAcc.saveErrors(new Path(outputPath, "id_errors.txt"))
        if (errorsMatches.nonEmpty)
          errorsMatches.saveErrors(new Path(outputPath, "match_errors.txt"))
      }

      val t1 = Timer.nanoClock()
      val elapsed = t1 - t0
      logger.info(f"All done in ${Timer.pretty(elapsed)}")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Uncaught exception", e)
        stopSparkAndExit(sc, exitCode = 500)
    }

    stopSparkAndExit(sc)
  }

}
