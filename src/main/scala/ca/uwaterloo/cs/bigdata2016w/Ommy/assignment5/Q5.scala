package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{DateChecker, Conf}
import org.apache.spark.{SparkContext, SparkConf}

object Q5 extends DateChecker{

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    val orders = sc.textFile(args.input() + "/orders.tbl")
    val customer = sc.textFile(args.input() + "/customer.tbl")
    val nation = sc.textFile(args.input() + "/nation.tbl")

    val shipDateColumn = 10

    val nationNameIdx = 1
    val customerNationKeyIdx = 3

    // Nation and Customer will fit into memory

    val customerMapping =
      sc.broadcast(
        customer
          .flatMap(line => {
            val tokens = line.split("\\|")
            // (custkey, nationkey)
            List((tokens.head, tokens(customerNationKeyIdx)))
          })
          .map((m) => (m._1, m._2))
          .collectAsMap())

    orders
      .flatMap(line => {
        val tokens = line.split("\\|")
        if (customerMapping.value.get(tokens(1)).get.equals("3")) {
          List((tokens.head, (tokens(1), "CANADA")))
        } else if (customerMapping.value.get(tokens(1)).get.equals("24")) {
          List((tokens.head, (tokens(1), "UNITED STATES")))
        } else {
          List()
        }
      })
      .map(m => (m._1, m._2))
      .cogroup(
        lineitem
          .map(line => {
            val tokens = line.split("\\|")
            // orderkey -> shipdate
            (tokens.head, tokens(shipDateColumn))
          })
      )
      .filter(f => f._2._2.nonEmpty && f._2._1.nonEmpty)
      .flatMap((m) => {
        val nation = m._2._1.toList.head._2
        m._2._2.map(x => (x.substring(0, 7), nation))
      })
      .map(m => ((m._1, m._2), 1))
      .reduceByKey(_+_)
      .sortBy(t => t._1._1)
      .collect()
      .foreach(f => {
        println("(" + f._1._1 + "," + f._2 + "," + f._1._2 + ")")
      })
  }
}