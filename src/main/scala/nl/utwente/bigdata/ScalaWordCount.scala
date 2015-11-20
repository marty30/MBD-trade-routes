package nl.utwente.bigdata;

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.mllib.clustering.{ LDA, DistributedLDAModel }
import org.apache.spark.mllib.linalg.Vectors
import tl.lin.data.map.HMapIIW
import org.apache.hadoop.io.IntWritable
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.Vector
import scala.collection.JavaConversions._
import scala.collection.mutable.WrappedArray

object ScalaWordCount {

  def main(args: Array[String]) {
    // configuration
    val conf = new SparkConf().setAppName(s"TestLDA")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")

    // spark context
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(1))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(2))

  }
}