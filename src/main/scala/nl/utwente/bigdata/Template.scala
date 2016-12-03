package nl.utwente.bigdata; // don't change classpath

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }

object Template {

  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName
    val inputDir = args(0)
    val outputDir = args(1)

    // configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer", "24")

    // spark context
    val sc = new SparkContext(conf)

    
    // actual program
    /* e.g. 
    val textFile = sc.textFile(inputDir)
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(outputDir)
    */
    
  }
}