package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment2

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

import scala.collection.mutable.ListBuffer

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
      textFile
        .flatMap(line => {
          val tokens = tokenize(line)
          if (tokens.length > 2) {
            val pair:List[(String, String)] = tokens.take(tokens.length).sliding(2).map(x => (x(0), x(1))).toList
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
        .reduceByKey(_+_)
        .repartitionAndSortWithinPartitions(pp)
        .mapPartitions((f) => {
          var wordCounts:Map[String, Float] = Map()
          var result:ListBuffer[(String, String, Float)] = ListBuffer()

          while (f.hasNext) {
            val i = f.next()
            if (i._1._2.contentEquals("*")) {
              wordCounts = wordCounts + (i._1._1 -> i._2)
              result = result :+ (i._1._1, i._1._2, i._2)
            } else {
              result = result :+ (i._1._1, i._1._2, (i._2/wordCounts(i._1._1)))
            }
          }
//
//          var x = 0
//          while (x < result.length) {
//            val res = result(x)
//            if (!res._2.contentEquals("*")) {
//              result(x) = (res._1, res._2, (res._3/wordCounts(res._1)))
//            }
//            x += 1
//          }

//          f.foreach((i) => {
//            if (i._1._2.contentEquals("*")) {
//              wordCounts = wordCounts + (i._1._1 -> i._2)
//            } else {
//              mapped = mapped :+ (i._1._1, i._1._2, i._2)
//              if (mapped.contains(i._1._1)) {
//                val at: Map[String, Float] = mapped(i._1._1) + (i._1._2 -> i._2)
//                mapped = mapped + (i._1._1 -> at)
//              } else {
//                val at: Map[String, Float] = Map(i._1._2 -> i._2)
//                mapped = mapped + (i._1._1 -> at)
//              }
//            }
//          })


//          var x = 0
//          while (x < mapped.length) {
//            val i = mapped(x)
//            result = result :+ ((i._1, i._2, (i._3 / wordCounts(i._1))))
//            x = x + 1
//          }

//          mapped.foreach((i) => {
//            result = result :+ ((i._1, i._2, (i._3 / wordCounts(i._1))))
//            i._2.foreach((x) => {
//              if (x._1.contentEquals("*")) {
//                result = result + ((i._1, x._1) -> (x._2))
//              } else {
//                result = result + ((i._1, x._1) -> (x._2 / wc))
//              }
//            })
//          })
//          wordCounts.foreach((i) => {
//            result = result :+ ((i._1, "*", i._2))
//          })
          result.iterator
        }, preservesPartitioning = false)
        .saveAsTextFile(args.output())
    } else {

    }
  }

}