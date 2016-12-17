package nl.utwente.bigdata; // don't change package name

import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkContext, SparkConf }
// uncomment if your program uses sql
//import org.apache.spark.sql.{ SQLContext }

object $toolName {

  def main(args: Array[String]) {
    // command line arguments
    val appName = this.getClass.getName
    
    // interpret command line, default: first argument is input second is output
    val inputDir = args(0)
    val outputDir = args(1)

    // configuration
    val conf = new SparkConf()
      .setAppName(s"$appName $inputDir $outputDir")

    // create spark context
    val sc = new SparkContext(conf)
    // uncomment if your program uses sql
    // val sqlContext = new SQLContext(sc)

    // add actual program here
    
  }
}