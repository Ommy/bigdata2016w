package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.Conf
import org.apache.spark.{SparkContext, SparkConf}

object Q7 {

  def compareDateLess(date:String, otherDate:String): Boolean = {
    // date is passed in
    // other date from tbl
    val passedIn = date.split("-")
    val tblDate = otherDate.split("-")
    for (i <- 0 to passedIn.length-1) {
      if (passedIn(i) < tblDate(i)) {
        return true
      }
    }
    false
  }

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
        if (compareDateLess(date, tokens(orderDateIdx))) {
          List((tokens(0), (tokens(orderDateIdx), tokens(shipPrioIdx), tokens(custKeyIdx))))
        } else {
          List()
        }
      })
      .map(m => (m._1, m._2))
      .join(
        lineitem
          .flatMap(line => {
            val tokens = line.split("\\|")
            if (!compareDateLess(date, tokens(shipDateIdx))) {
              List((tokens(0), tokens(5).toDouble * (1 - tokens(6).toDouble)))
            } else {
              List()
            }
          }).map(m => (m._1, m._2)))
      .map(m => {
        (m._1, m._2)
      })
      .reduceByKey((x,y) => {
        (x._1, x._2 + y._2)
      }, numPartitions = 10)
      .mapPartitions(m => {
        m.map(f => {
          (customer.value.get(f._2._1._3).get, f._1, f._2._2, f._2._1._1, f._2._1._2)
        })
      })
      .groupBy(f => (f._1, f._2, f._4, f._5))
      .mapPartitions(m => m.map(x => (x._1, x._2.head)))
      .sortBy(s => s._2._3, ascending = false)
      .take(10)
      .foreach(f => {
        println("(" + f._1._1 + "," + f._1._2 + "," + f._1._3 + "," + f._1._4 + "," + f._2._3 + ")")
      })
//      .saveAsTextFile(args.output())
//      .mapPartitions(m => {
//        println(m.toString)
//        m.map(f => {
//          println(f._1 + " -- " + f._2)
//        })
//      })
  }
}
