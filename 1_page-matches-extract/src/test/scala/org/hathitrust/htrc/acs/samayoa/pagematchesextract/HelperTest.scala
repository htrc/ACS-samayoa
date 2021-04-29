package org.hathitrust.htrc.acs.samayoa.pagematchesextract

import org.hathitrust.htrc.acs.samayoa.pagematchesextract.Helper.extractFullPageSentences
import org.hathitrust.htrc.data.{HtrcPage, HtrcVolume, HtrcVolumeId}
import org.scalamock.scalatest.MockFactory
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HelperTest extends AnyFunSuite with OptionValues with Matchers with MockFactory {

  test("Extract full page sentences") {
    val pages = IndexedSeq(
      new HtrcPage("1",
        """
          |   This is the first
          |sentence.  This is
          |the second sen-
          |tence, and it's nice.
          |
          |   This is the third
          |""".stripMargin),
      new HtrcPage("2",
        """
          |sentence. I am running
          |out of ideas.
          |""".stripMargin),
      new HtrcPage("3", ""),
      new HtrcPage("4",
        """
          |This is the fifth sentence.""".stripMargin),
      new HtrcPage("5", "")
    )

    val vol = new HtrcVolume(new HtrcVolumeId("mdp.1234"), pages)

    val sentences = extractFullPageSentences(vol).toArray

    sentences(0)._2 should contain theSameElementsInOrderAs List(
      """This is the first
        |sentence.""".stripMargin,
      """This is
        |the second sentence,
        |and it's nice.""".stripMargin,
      """This is the third sentence."""
    )

    sentences(1)._2 should contain theSameElementsInOrderAs List(
      """I am running
        |out of ideas.""".stripMargin
    )

    sentences(2)._2 should contain theSameElementsInOrderAs List()

    sentences(3)._2 should contain theSameElementsInOrderAs List(
      """This is the fifth sentence."""
    )

    sentences(4)._2 should contain theSameElementsInOrderAs List()
  }

}
