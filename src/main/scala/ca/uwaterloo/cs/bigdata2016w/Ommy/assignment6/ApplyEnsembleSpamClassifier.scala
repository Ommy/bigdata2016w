package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment6

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}


object ApplyEnsembleSpamClassifier {

  def genMap(name: String, sc: SparkContext): collection.Map[String, Double] = {
    sc
      .textFile(name)
      .map(x => {
        val tokens = x.replace("(","").replace(")","").split(",")
        (tokens(0), tokens(1).toDouble)
      })
      .collectAsMap()
  }

  def getForAll(X:collection.Map[String, Double], Y:collection.Map[String, Double], B:collection.Map[String, Double], key:String): Double = {
    X.getOrElse(key, 0.0) + Y.getOrElse(key, 0.0) + B.getOrElse(key, 0.0)
  }

  def getTuple(X:collection.Map[String, Double], Y:collection.Map[String, Double], B:collection.Map[String, Double], key:String): (Double, Double, Double) = {
    (X.getOrElse(key, 0.0), Y.getOrElse(key, 0.0), B.getOrElse(key, 0.0))
  }

  def getVote(value:Double): Int = {
    if (value.compareTo(0.0) > 0) {
      1
    } else if (value.compareTo(0.0) == 0){
      0
    } else {
      -1
    }
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("ApplyEnsembleClassifier")
    val sc = new SparkContext(conf)

    val group_x = sc.broadcast(genMap(args.model() + "/part-00000", sc))
    val group_y = sc.broadcast(genMap(args.model() + "/part-00001", sc))
    val group_b = sc.broadcast(genMap(args.model() + "/part-00002", sc))

    val method = args.method()

    sc
      .textFile(args.input())
      .map(line => {
        val tokens = line.split(" ")
        (tokens(0), (tokens(1), tokens.slice(2, tokens.size)))
      })
      .map(f => {
        val result:Double = f._2._2.map(x => getForAll(group_x.value, group_y.value, group_b.value, x)).sum
        if (method.equals("average")) {
          val div:Double = result / 3.0
          if (div.compareTo(0.0) > 0) {
            (f._1, f._2._1, result, "spam")
          } else {
            (f._1, f._2._1, result, "ham")
          }
        } else {
          val resultTuple:(Double, Double, Double) = f._2._2
                    .map(x => getTuple(group_x.value, group_y.value, group_b.value, x))
                    .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))
          val vote:Integer = getVote(resultTuple._1) + getVote(resultTuple._2) + getVote(resultTuple._3)
          if (vote > 0) {
            (f._1, f._2._1, vote, "spam")
          } else {
            (f._1, f._2._1, vote, "ham")
          }
        }
      })
      .saveAsTextFile(args.output())

  }
}
