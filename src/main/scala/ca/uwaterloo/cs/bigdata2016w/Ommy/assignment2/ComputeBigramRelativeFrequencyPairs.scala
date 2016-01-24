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

    if (!args.imc()) {
      val kk = textFile
        .flatMap(line => {
          val tokens = tokenize(line)
          if (tokens.length > 2) {
            val pair:List[(String, String)] = tokens.take(tokens.length-1).sliding(2).map(x => (x(0), x(1))).toList
            val starPairs:List[(String, String)] = tokens.take(tokens.length-1).map(word => (word, "*"))
            pair ::: starPairs
          } else {
            List()
          }
        })
//        .map((i) => ((i._1, i._2), 1.0f))
        .mapPartitions((iter) => {
          iter.map((f) => {
            ((f._1, f._2), 1.0f)
          })
        })
        .reduceByKey(partitioner = pp, _+_)
        .mapPartitions((f) => {
          var mapped:Map[String, Map[String, Float]] = Map()
          f.foreach((i) => {
            if (mapped.contains(i._1._1)) {
              val at:Map[String, Float] = mapped(i._1._1) + (i._1._2 -> i._2)
              mapped =  mapped + (i._1._1 -> at)
            } else {
              val at:Map[String, Float] = Map(i._1._2 -> i._2)
              mapped = mapped + (i._1._1 -> at)
            }
          })
          var result:Map[(String, String), Float] = Map()

          mapped.foreach((i) => {
            val wc:Float = i._2("*")
            i._2.foreach((x) => {
              if (x._1.contentEquals("*")) {
                result = result + ((i._1, x._1) -> (x._2))
              } else {
                result = result + ((i._1, x._1) -> (x._2 / wc))
              }
            })
          })
          result.iterator
        }, preservesPartitioning = false)
        .saveAsTextFile(args.output())
    } else {

    }
  }

}