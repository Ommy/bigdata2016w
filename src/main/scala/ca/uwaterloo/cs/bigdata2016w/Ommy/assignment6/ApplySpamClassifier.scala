package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment6

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}


object ApplySpamClassifier {

  def main (argv: Array[String]){
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val model = sc.broadcast(sc.textFile(args.model()+"/part-00000").map(f => {
      val tokens = f.split(",")
      (tokens(0).replace("(", ""), tokens(1).replace(")", "").toDouble)
    }).collectAsMap())

    sc
    .textFile(args.input())
    .map(line => {
      val tokens = line.split(" ")
      (tokens(0), (tokens(1), tokens.slice(2, tokens.size)))
    }).map(f => {
      val res = f._2._2.map(x => model.value.getOrElse(x, 0.0)).sum
      if (res > 0) {
        (f._1, f._2._1, res, "spam")
      } else {
        (f._1, f._2._1, res, "ham")
      }
    })
    .saveAsTextFile(args.output())

  }

}
