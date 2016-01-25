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
      .map((i) => ((i._1, i._2), 1.0f))
      .reduceByKey(_+_)
      .repartitionAndSortWithinPartitions(pp)
      .mapPartitions((f) => {
        var wc:Float = 0.0f
        f.map((i) => {
          if (i._1._2 == "*") {
            wc = i._2
            ((i._1._1, i._1._2), i._2)
          } else {
            ((i._1._1, i._1._2), i._2/wc)
          }
        })
      }, preservesPartitioning = false)
      .saveAsTextFile(args.output())
  }

}
