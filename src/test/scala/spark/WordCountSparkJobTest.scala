package spark


import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

/**
 * Created by renienj on 11/29/15.
 */
class WordCountSparkJobTest extends FlatSpec with Matchers with BeforeAndAfter {

  private var sc: Some[SparkContext] = null

  private def sampleData1: RDD[String] = {
    val seq = Seq(
      "I love spark",
      "I love hadoop",
      "hadoop is awesome",
      "scala is best and awesome")
    sc.map(_.parallelize(seq)).get
  }

  "WordCountSparkJob" should
    "should create the tokens" in {
    val processed = WordCountSparkJob.process(sampleData1)
    assert(processed.count() == 9)
  }

  it should "provide following results" in {
    val processed = WordCountSparkJob.process(sampleData1)
    val results = processed.collect()
    assert(results(0) == ("scala",1))
    assert(results(1) == ("is",2))
    assert(results(2) == ("love",2))
    assert(results(3) == ("best",1))
    assert(results(4) == ("spark",1))
    assert(results(5) == ("hadoop",2))
    assert(results(6) == ("I",2))
    assert(results(7) == ("awesome",2))
    assert(results(8) == ("and",1))
  }

  after {
    sc.foreach(_.stop())
  }

  before {
    val conf = new SparkConf().setAppName("WordCountSparkJobTest").setMaster("local[2]")
    sc = Some(SparkContext.getOrCreate(conf))
  }
}
