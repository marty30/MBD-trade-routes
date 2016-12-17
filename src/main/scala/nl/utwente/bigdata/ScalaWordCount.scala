package nl.utwente.bigdata;

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

object ScalaWordCount {

  /* 
     function that takes a list of texts and returns the counts
     of individual words 
  */
  def count(lines: RDD[String]) : RDD[(String,Int)] = {
    val counts = lines.flatMap(line => line.split("\\W+"))
      .map(word => (word, 1))
      .reduceByKey((a,b) => a+b)
    counts
  }

  /*
   Script to load text documents from disk and store the counts
   in a set of textfiles
   */
  def main(args: Array[String]) {
    // configuration
    val conf = new SparkConf().setAppName(s"TestLDA")

    // spark context
    val sc = new SparkContext(conf)

    count(sc.textFile(args(0))).saveAsTextFile(args(1))
  }
}