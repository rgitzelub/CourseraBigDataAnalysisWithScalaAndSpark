package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.scalatest.Matchers._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import StackOverflow._

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FlatSpec with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  "testObject" should "be instantiated" in {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }


  val noPostings = sc.parallelize(List[Posting]())

  val noAnswer = sc.parallelize(List(
    Posting(1, 100, None, None, 0, None)
  ))

  val oneQuestionOneAnswer = sc.parallelize(List(
    Posting(1, 100, Some(200), None, 0, None),
    Posting(2, 200, None, Some(100), 20, None)
  ))

  val oneQuestionThreeAnswers = sc.parallelize(List(
    Posting(1, 100, Some(200), None, 0, None),
    Posting(2, 200, None, Some(100), 20, None),
    Posting(2, 300, None, Some(100), 30, None),
    Posting(2, 400, None, Some(100), 40, None)
  ))

  val twoQuestionsFiveAnswers = sc.parallelize(List(
    Posting(1, 100, Some(200), None, 0, None),
    Posting(2, 200, None, Some(100), 20, None),
    Posting(2, 300, None, Some(100), 30, None),
    Posting(2, 400, None, Some(100), 40, None),
    Posting(1, 500, Some(700), None, 0, None),
    Posting(2, 600, None, Some(500), 60, None),
    Posting(2, 700, None, Some(500), 70, None)
  ))

  "groupedPostings" should "work on empty list" in {
     groupedPostings(noPostings).count should be (0)
  }

  "groupedPostings" should "ignore unanswered questions" in {
    val results = groupedPostings(noAnswer)
    results.count should be (0)
  }

  "groupedPostings" should "find single question and answer" in {
    val results = groupedPostings(oneQuestionOneAnswer).collect
    results.size should be (1)
    results(0)._1 should be (100)
    results(0)._2.size should be (1)
  }

  "groupedPostings" should "find single question and multiple answers" in {
    val results = groupedPostings(oneQuestionThreeAnswers).collect
    results.size should be (1)
    results(0)._1 should be (100)
    results(0)._2.size should be (3)
  }

  "groupedPostings" should "find multiple questions and multiple answers" in {
    val results = groupedPostings(twoQuestionsFiveAnswers).collect
    results.size should be (2)
    results(0)._1 should be (100)
    results(0)._2.size should be (3)
    results(1)._1 should be (500)
    results(1)._2.size should be (2)
  }



  "scoredPostings" should "work on empty list" in {
    scoredPostings(groupedPostings(noPostings)).count should be (0)
  }

  "scoredPostings" should "ignore unanswered questions" in {
    scoredPostings(groupedPostings(noAnswer)).count should be (0)
  }

  "scoredPostings" should "find single question and answer" in {
    val results = scoredPostings(groupedPostings(oneQuestionOneAnswer)).collect
    results.size should be (1)
    results(0)._1.id should be (100)
    results(0)._2 should be (20)
  }

  "scoredPostings" should "find single question and multiple answers" in {
    val results = scoredPostings(groupedPostings(oneQuestionThreeAnswers)).collect
    results.size should be (1)
    results(0)._1.id should be (100)
    results(0)._2 should be (40)
  }

  "scoredPostings" should "find multiple questions and multiple answers" in {
    val results = scoredPostings(groupedPostings(twoQuestionsFiveAnswers)).collect
    results.size should be (2)
    results(0)._1.id should be (100)
    results(0)._2 should be (40)
    results(1)._1.id should be (500)
    results(1)._2 should be (70)
  }

}
