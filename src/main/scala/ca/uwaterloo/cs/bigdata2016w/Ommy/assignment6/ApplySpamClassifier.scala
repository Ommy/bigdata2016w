package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment6

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}


object ApplySpamClassifier {

  def main (argv: Array[String]){
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

  }

}
