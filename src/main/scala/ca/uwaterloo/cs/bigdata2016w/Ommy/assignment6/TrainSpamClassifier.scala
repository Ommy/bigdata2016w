package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment6

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}
import scala.math._
import scala.util.Random


object TrainSpamClassifier {

  // w is the weight vector (make sure the variable is within scope)
  var w = Map[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main (argv: Array[String]): Unit = {

    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args.input())

    // This is the main learner:
    val delta:Double = 0.002

    // Indexes
    val DOCID_IDX = 0
    val LABEL_IDX = 1
    val FEATURES_IDX = 2

    val doRandom:Boolean = args.shuffle()

    val x = textFile.map(line =>{
      val tokens = line.split(" ")
      var sortedOn = 0
      val random = Random
      if (doRandom) {
        sortedOn = random.nextInt(Integer.MAX_VALUE)
      }
      (0, (tokens(DOCID_IDX), tokens(LABEL_IDX), tokens.slice(FEATURES_IDX, tokens.length).map(x => x.toInt), sortedOn))
    })
    .sortBy(x => x._2._4)
    .groupByKey(1)
    .flatMap(x => {
      // Update the weights as follows:
      x._2.foreach(p => {
        val score = spamminess(p._3)
        val prob:Double = 1.0 / (1 + exp(-score))
        val isSpam:Double = if (p._2.equals("spam")) { 1.0 } else { 0.0 }
        p._3.foreach(f => {
          if (w.contains(f)) {
            w = w + (f -> (w(f) + ((isSpam - prob) * delta)))
          } else {
            w = w + (f -> ((isSpam - prob) * delta))
          }
        })
      })
      w.flatMap(d => {
        List((d._1, d._2).toString())
      })
    })
    .saveAsTextFile(args.model())

  }

}