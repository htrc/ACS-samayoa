package org.hathitrust.htrc.acs.samayoa.pagematchesextract

import edu.stanford.nlp.pipeline.{CoreDocument, StanfordCoreNLP}
import org.hathitrust.htrc.data.HtrcVolume
import org.hathitrust.htrc.data.ops.TextOptions
import org.hathitrust.htrc.tools.scala.io.IOUtils.using
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException
import java.util.{Locale, Properties}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object Helper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(Main.appName)

  private def loadPropertiesFromClasspath(path: String): Try[Properties] = {
    require(path != null && path.nonEmpty)

    Option(getClass.getResourceAsStream(path))
      .map(using(_) { is =>
        Try {
          val props = new Properties()
          props.load(is)
          props
        }
      })
      .getOrElse(Failure(new FileNotFoundException(s"$path not found")))
  }

  private def getLocalePropertiesFromClasspath(locale: Locale): Properties = {
    val lang = locale.getLanguage
    val langProps = s"/nlp/config/$lang.properties"
    logger.debug(s"Loading ${locale.getDisplayLanguage} settings from $langProps")
    loadPropertiesFromClasspath(langProps) match {
      case Success(p) => p
      case Failure(e) => throw e
    }
  }

  private def createNLPInstance(props: Properties): StanfordCoreNLP =
    new StanfordCoreNLP(props)

  private val nlpPipeline: StanfordCoreNLP = createNLPInstance(getLocalePropertiesFromClasspath(Locale.US))

  def extractFullPageSentences(vol: HtrcVolume): Array[(String, mutable.Buffer[String])] = {
    val pagesSentences = vol.structuredPages.view
      .map { p =>
        val text = p.body(TextOptions.TrimLines, TextOptions.RemoveEmptyLines, TextOptions.DehyphenateAtEol).trim
        val doc = new CoreDocument(text)
        nlpPipeline.annotate(doc)
        p.seq -> doc.sentences().iterator().asScala.map(_.text()).toBuffer
      }
      .toArray

    // augment the list of pages with a copy of the last page so that the `.sliding(2)` method above can be mapped
    // over, such that for each pair of subsequent pages we can yield the adjusted page (with the first sentence
    // from the next page added to it)
    val augmentedPagesSentences =
    if (pagesSentences.nonEmpty) pagesSentences :+ pagesSentences.last
    else pagesSentences

    augmentedPagesSentences.sliding(2)
      .map {
        // if the pair represents two different pages
        case Array(pageA@(seqA, sentA), (seqB, sentB)) if seqA != seqB =>
          if (sentA.isEmpty || sentB.isEmpty) pageA
          else {
            sentA(sentA.size - 1) = sentA.last + " " + sentB.remove(0)
            pageA
          }

        // if the pair represents the "same" page (as added before)
        case Array(pageA, _) => pageA
      }
      .toArray
  }

  def countOccurrences(patterns: Seq[Regex], text: String): Seq[Int] = patterns.map(_.findAllMatchIn(text).size)

}
