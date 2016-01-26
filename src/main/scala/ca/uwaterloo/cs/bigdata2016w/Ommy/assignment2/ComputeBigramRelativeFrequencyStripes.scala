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

    val textFile = sc.textFile(args.input())

    if (!args.imc()) {
      textFile
        .flatMap(line => {
//          var stripes:Map[String, Map[String, Float]] = Map()
//          if (tokens.length > 2) {
//            for (x <- 1 to tokens.length-1) {
//              val prev = tokens(x-1)
//              val curr = tokens(x)
//              if (stripes.contains(prev)) {
//                var stripe:Map[String, Float] = stripes(prev)
//                if (stripe.contains(curr)) {
//                  val count:Float = stripe(curr)
//                  stripe = stripe + (curr -> (count + 1.0f))
//                  stripes = stripes + (curr -> stripe)
//                } else {
//                  stripe = stripe + (curr -> 1.0f)
//                  stripes = stripes + (curr -> stripe)
//                }
//              } else {
//                val stripe:Map[String, Float] = Map(curr -> 1.0f)
//                stripes = stripes + (prev -> stripe)
//              }
//            }
          val tokens:List[String] = tokenize(line)
          var stripes:Map[String, Map[String, Float]] = Map()
          tokens.sliding(2).take(tokens.length-1).foreach((x) => {
            if (!stripes.contains(x.head)) {
              stripes += (x.head -> Map(x(1) -> 1.0f))
            } else {
              stripes += (x.head -> (stripes(x.head) + (x(1) -> (1.0f + stripes(x.head).getOrElse(x(1), 0.0f)))))
            }
          })
//          if (tokens.length > 2) {
//            for (x <- 1 to tokens.length-1) {
//              val prev = tokens(x-1)
//              val curr = tokens(x)
//              if (stripes.contains(prev)) {
//                var stripe:Map[String, Float] = stripes(prev)
//                if (stripe.contains(curr)) {
//                  val count:Float = stripe(curr)
//                  stripe = stripe + (curr -> (count + 1.0f))
//                  stripes = stripes + (curr -> stripe)
//                } else {
//                  stripe = stripe + (curr -> 1.0f)
//                  stripes = stripes + (curr -> stripe)
//                }
//              } else {
//                val stripe:Map[String, Float] = Map(curr -> 1.0f)
//                stripes = stripes + (prev -> stripe)
//              }
//            }
          stripes
        })
        .map(m => {
          (m._1, m._2)
        })
        .reduceByKey((x, y) => {
          x ++ y.map{ case (k,v) => k -> (v + x.getOrElse(k, 0.0f)) }
        })
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
//          val key:String = item._1
//          val maps:Map[String, Float]] = item._2
//
//          var result:Map[String, Float] = Map()
//
//          maps.foreach((f) => {
//            f.foreach((m) => {
//              if (result.contains(m._1)) {
//                val t:Float = result(m._1)
//                result = result + (m._1 -> (t+m._2))
//              } else {
//                result = result + m
//              }
//            })
//          })
//
//          var sum:Float = 0.0f
//          result.foreach((f) => {
//            sum = sum + f._2
//          })
//
//          result.foreach((f) => {
//            val t:Float = f._2
//            result = result + (f._1 -> (t/sum))
//          })
//          (key, result)
        })
//        .reduceByKey((a:Map[String, Float],b:Map[String, Float]) => {
//          a
//            .toSeq.++(b.toSeq)
//            .groupBy(_._1)
//            .mapValues(_.map(_._2).reduce(_+_))

//          var sum:Float = 0.0f
//          merged.foreach(element => {
//            sum = sum + element._2
//          })
//          var result:Map[String, Float] = Map()
//          for (x <- merged.keys) {
//            result = result + (x -> sum)
//          }
//          result

//        }).groupByKey()
//        .map(w => {
//          val merged = w._2
//          var sum:Float = 0.0f
//          merged.foreach(element => {
//            sum = sum + element.
//          })
//          var result:Map[String, Float] = Map()
//          for (x <- merged.keys) {
//            result = result + (x -> (merged(x)/sum))
//          }
//
//        })
        .saveAsTextFile(args.output())
    } else {

    }
  }

}
