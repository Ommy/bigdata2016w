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

    val myPartitioner = new MyPartitioner(args.reducers())

    val shipDateColumn = 10

    val nationNameIdx = 1
    val customerNationKeyIdx = 3

    // Nation and Customer will fit into memory

    val nationMapping =
      sc.broadcast(
        nation
          .flatMap(line => {
            val tokens = line.split("\\|")
            List((tokens.head, tokens(nationNameIdx)))
          })
          .map((m) => (m._1, m._2))
          .collectAsMap())

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
        val nation = nationMapping.value.get(customerMapping.value.get(tokens(1)).get).get
        if (List("CANADA", "UNITED STATES") contains nation) {
          // orderkey -> (custkey, nation)
          List((tokens.head, (tokens(1), nation)))
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
      .foreach(f => {
        println(f._1._1 + "," + f._2 + "," + f._1._2)
      })


//    orders
//      .flatMap(line => {
//        val tokens = line.split("\\|")
//        // (orderkey, custkey)
//        List((tokens.head, tokens(1)))
//      })
//      .map((m) => (m._1, m._2))
//      .cogroup(
//        lineitem
//        .flatMap(line => {
//          val tokens:Array[String] = line.split("\\|")
//          val shipDate = tokens(shipDateColumn)
//          List((tokens.head, shipDate))
//        })
//        // (orderkey, 1)
//        .map((m) => (m._1, m._2))
//      )
//      .filter((f) => f._2._2.toList.nonEmpty)
//      .map((m) => {
//        val nationkey = customerMapping.value.get(m._2._1.toList.head).get
//        if (nationMapping.value.get(nationkey).get.equals())
//        ((nationkey, nationMapping.value.get(nationkey).get) , 1)
//      })
//      .reduceByKey(_+_)
//      .repartitionAndSortWithinPartitions(myPartitioner)
//      .sortBy((f) => f._1._1.toInt)
//      .foreach((f) => {
//        println("(" + f._1._1 + "," + f._1._2 + "," + f._2 + ")")
//      })
  }
}