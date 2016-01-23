package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment2

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

class PairPartitioner( numberOfReducers: Int) extends Partitioner {

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, String)]
    return (k._1.hashCode() & Int.MaxValue) % numberOfReducers
  }

  override def numPartitions: Int = {
    return numberOfReducers
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  var log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)

    val conf = new SparkConf().setAppName("BigramRelativeFrequencyPairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    val pp = new PairPartitioner(args.reducers())
    val textFile = sc.textFile(args.input())
    var wcMap:Map[String, Int] = Map()

    if (!args.imc()) {
      textFile
        .flatMap(line => {
          val tokens = tokenize(line)
          if (tokens.length > 2) {
            val pair = tokens.sliding(2).map(x => (x(0), x(1))).toList
            val starPairs = tokens.map(word => (word, "*"))
            pair ::: starPairs
          } else {
            List()
          }
        })
        .map(word => {
          (word, 1)
        })
        .reduceByKey(pp, (a,b) => {
          a + b
        }).groupByKey(pp)
        .map(a => {
          val key = a._1
          val value = a._2.toList
          var margin = 0
          var sum:Int = 0
          for(x <- 0 to value.length-1) {
            sum = sum + value(x)
          }

          if (key._2.contains("*")) {
//            margin = sum
            wcMap = wcMap ++ Map(key._1 -> sum)
            (key, sum)
          } else {
            val margin:Int = wcMap(key._1)
            (key, sum / margin)
          }
        })
//        .reduceByKey(_+_)
//        .repartitionAndSortWithinPartitions(pp)
//        .groupByKey
//        .map(iter => {
//          val key = iter._1
//          val list = iter._2.toList
//          var sum = 0
//          for (x <- 0 to list.length-1) {
//            sum = sum + list(x)
//          }
//
//          println("@@@@@@: " + key + " ******: " + list)
//
//          if (key._2.contentEquals("*")) {
//            (key, sum)
//          } else {
//            (key, sum)
//          }
//        })
//        .reduceByKey(pp, (a,b) => {
//          a + b
//        })
        .saveAsTextFile(args.output())
    } else {

    }
  }

}