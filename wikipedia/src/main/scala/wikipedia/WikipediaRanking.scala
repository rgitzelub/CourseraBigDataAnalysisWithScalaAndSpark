package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("My app")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

  /** Returns the number of articles on which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd.filter(_.mentionsLanguage(lang)).count.toInt


  protected def articlesForLang(langs: List[String], rdd: RDD[WikipediaArticle]): Map[String, RDD[WikipediaArticle]] =
    langs.foldLeft(Map[String,RDD[WikipediaArticle]]()){ (m,lang) =>
      m + (lang -> rdd.filter(_.mentionsLanguage(lang)))
    }

  protected def countsForLang(langs: List[String], rdd: RDD[WikipediaArticle]): Map[String,Int] =
    articlesForLang(langs, rdd).mapValues(_.count.toInt)

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    countsForLang(langs, rdd).toList.sortBy(_._2).reverse

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
//  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
//    // okay, easy enough to figure out which languages each article refers to
//    val langsForArticles = rdd
//      .map { article =>
//          (article, langs.filter(article.mentionsLanguage))
//        }
//      .filter(_._2.nonEmpty)
//
//    // but now what?  I really need to *flatMap* here, to break out a tuple for every lang, but how do I create a new RDD?
//    //  isn't there something magic in RDD?  Or... is it in the Context?
//    // oh!!! I don't have to!  RDD.flatMap doesn't generate an RDD, just a collection! weird...
//    val langsAndArticles = langsForArticles.flatMap { case (article, listOfLangs) => // wtf? can't name the args?
//      listOfLangs.map((article, _))
//    }
//    // group that by lang, and then remove the now-redundant tuple
//    val x = langsAndArticles
//      .groupBy(_._2)
//      .map{ case(lang, listOfArticles) =>
//        (lang, listOfArticles.map(_._1))
//      }
//    // tada!
//    x
//  }

  // both versions run about the same:
  //   TIMING: Processing Part 1: naive ranking took 8170 ms.
  //     Processing Part 2: ranking using inverted index took 7114 ms.

  // interesting what happens if you .persist the RDD up at the top
  //   TIMING: Processing Part 1: naive ranking took 2372 ms.
  //   Processing Part 2: ranking using inverted index took 8422 ms.


  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =
    rdd
      // enumerate all combinations
      .flatMap { article =>
        langs.filter(article.mentionsLanguage).map((_, article))
      }
      // make lists of articles for each language
      .groupBy(_._1)
      // remove the now-redundant tuples
      .map{ case(lang, listOfLangArticlePairs) =>
        (lang, listOfLangArticlePairs.map(_._2))
      }


  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] =
    index
      .mapValues(_.size)
      .collect()
      .sortBy(_._2)
      .reverse
      .toList



  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] =
    rdd
      // enumerate all combinations, but don't keep the article, just a count
      //  (it seems weird, but we need a list of tuples for reduceByKey)
      .flatMap { article =>
        langs.filter(article.mentionsLanguage).map((_, 1))
      }
      // add up all the 1's
      .reduceByKey {_ + _}
      .collect()
      .toList
      .sortBy(_._2)
      .reverse

  /*
   * that's definitely quicker!
      TIMING: Processing Part 1: naive ranking took 8157 ms.
      Processing Part 2: ranking using inverted index took 6563 ms.
      Processing Part 3: ranking using reduceByKey took 1207 ms.
   */

  /*
   * whoops!!! I was doing it wrong!!  I submitted and got 7/14... turns out
   *  I was doing just the string 'contains' when I should have been looking for
   *  a separate word.  D'uh:  'Java' would also match "Javascript" incorrectly
   *
   * but this is a bit slower, just for splitting all the strings...
       TIMING: Processing Part 1: naive ranking took 18387 ms.
      Processing Part 2: ranking using inverted index took 16467 ms.
      Processing Part 3: ranking using reduceByKey took 10899 ms.

   */
  def main(args: Array[String]) {


//    println(countsForLang(langs, wikiRdd))


    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    println(langsRanked)
    println(langsRanked2)
    println(langsRanked3)

    /* Output the speed of each ranking */
    println("TIMING: " + timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}

