package spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
 * Created by renienj on 11/29/15.
 */

//Need to modify the job
object WordCountSparkJob {

  def process(lines: RDD[String]): RDD[(String, Int)] = {
    lines.flatMap(line => line.split(" ")).
      map(word => (word, 1)).reduceByKey(_ + _)
  }

  def main(args: Array[String]) {
    // set spark context
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val distFile = sc.textFile(args(0))
    process(distFile).saveAsTextFile(args(1))

  }
}
