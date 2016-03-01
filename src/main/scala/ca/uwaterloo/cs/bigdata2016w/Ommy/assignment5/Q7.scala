package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}

object Q7 {

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val date = args.date()

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl", 10)
    val orders = sc.textFile(args.input() + "/orders.tbl", 10)

    val customer =
      sc.broadcast(
        sc.textFile(args.input() + "/customer.tbl", 10)
        .map(line => {
          val tokens = line.split("\\|")
          // (custkey, name)
          (tokens(0), tokens(1))
        })
        .collectAsMap())

    val orderDateIdx = 4
    val shipPrioIdx = 7
    val custKeyIdx = 1
    val shipDateIdx = 10


    // lineitem won't fit in memory
    // orders won't fit in memory
    // customer will fit in memory
    orders
      .flatMap(line => {
        val tokens = line.split("\\|")
        if (date > tokens(orderDateIdx)) {
          List((tokens(0), (tokens(orderDateIdx), tokens(shipPrioIdx), tokens(custKeyIdx))))
        } else {
          List()
        }
      })
      .map(m => (m._1, m._2))
      .cogroup(
        lineitem
          .flatMap(line => {
            val tokens = line.split("\\|")
            if (date < tokens(shipDateIdx)) {
              List((tokens(0), tokens(5).toDouble * (1.0 - tokens(6).toDouble)))
            } else {
              List()
            }
          }).map(m => (m._1, m._2)))
      .flatMap(m => {
        if (m._2._2.nonEmpty && m._2._1.nonEmpty) {
          val orders = m._2._1.head
          m._2._2.flatMap(f => {
            List(((customer.value.get(orders._3).get, m._1, orders._1, orders._2), f))
          })
        } else {
          List()
        }
      })
      .reduceByKey(_+_, numPartitions = 10)
      .sortBy(s => s._2, ascending = false)
      .take(10)
      .foreach(f => {
        println("(" + f._1._1 + "," + f._1._2 + "," + f._2 + "," + f._1._3 + "," + f._1._4 + ")")
      })
  }
}
