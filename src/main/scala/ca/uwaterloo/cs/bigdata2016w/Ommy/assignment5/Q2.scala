package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{DateChecker, Conf}
import org.apache.spark.{SparkContext, SparkConf}

object Q2 extends DateChecker{

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl", 10)
    val orders = sc.textFile(args.input() + "/orders.tbl", 10)
    val shipDateColumn = 10
    val orderKeyColumn = 0
    val clerkColumn = 6
    val date = args.date()

    orders
      .flatMap(line => {
        val tokens = line.split("\\|")
        List((tokens(orderKeyColumn), tokens(clerkColumn)))
      })
      .map((m) => (m._1, m._2))
      .cogroup(
        lineitem
          .flatMap(line => {
            val tokens:Array[String] = line.split("\\|")
            val shipDate = tokens(shipDateColumn)
            if (checkDate(date, shipDate)) {
              List((tokens(orderKeyColumn), shipDate))
            } else {
              List()
            }
          })
          .map((m) => (m._1, m._2)))
      .filter((f) => f._2._2.toList.nonEmpty)
      .mapPartitions((m) => {
        m.map(f => {
          (f._1, f._2._1.toList.head)
        })
      })
      .sortBy(f => f._1.toInt)
      .take(20)
      .foreach((f) => {
        println("(" + f._1 + "," + f._2 + ")")
      })

  }

}