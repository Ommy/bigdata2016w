package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment2

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Tokenizer
import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

class StripePartitioner( numberOfReducers: Int) extends Partitioner {

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[String]
    return (k.hashCode() & Int.MaxValue) % numberOfReducers
  }

  override def numPartitions: Int = {
    return numberOfReducers
  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  var log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)

    val conf = new SparkConf().setAppName("BigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)
    val pp = new StripePartitioner(args.reducers())

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())

    if (!args.imc()) {
      textFile
        .flatMap(line => {
          val tokens:List[String] = tokenize(line)
          var stripes:Map[String, Map[String, Float]] = Map()
          tokens.sliding(2).take(tokens.length-1).foreach((x) => {
            if (!stripes.contains(x.head)) {
              stripes += (x.head -> Map(x(1) -> 1.0f))
            } else {
              stripes += (x.head -> (stripes(x.head) + (x(1) -> (1.0f + stripes(x.head).getOrElse(x(1), 0.0f)))))
            }
          })
          stripes
        })
        .map(m => {
          (m._1, m._2)
        })
        .reduceByKey((x, y) => {
          x ++ y.map{ case (k,v) => k -> (v + x.getOrElse(k, 0.0f)) }
        }, args.reducers())
        .repartitionAndSortWithinPartitions(pp)
        .mapPartitions(item => {
          item.map((f) => {
            var sum:Float = 0
            f._2.foreach((i) => {
              sum += i._2
            })
            f._2.map((i) => {
              ((f._1, i._1), i._2 / sum)
            })
          })
        })
        .saveAsTextFile(args.output())
    } else {

    }
  }

}
