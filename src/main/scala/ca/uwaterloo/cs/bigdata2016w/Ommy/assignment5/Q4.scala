package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{DateChecker, Conf}
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

class MyPartitioner( numberOfReducers: Int) extends Partitioner {

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[(String, String)]
    return (k._1.toInt.hashCode() & Int.MaxValue) % numberOfReducers
  }

  override def numPartitions: Int = {
    return numberOfReducers
  }
}

object Q4 extends DateChecker{

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl", 10)
    val orders = sc.textFile(args.input() + "/orders.tbl", 10)
    val customer = sc.textFile(args.input() + "/customer.tbl", 10)
    val nation = sc.textFile(args.input() + "/nation.tbl", 10)

    val myPartitioner = new MyPartitioner(10)

    val date = args.date()
    val shipDateColumn = 10

    val nationNameIdx = 1
    val customerNationKeyIdx = 3

    // Nation and Customer will fit into memory
    /**
     * select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation
        where
          l_orderkey = o_orderkey and
          o_custkey = c_custkey and
          c_nationkey = n_nationkey and
          l_shipdate = 'YYYY-MM-DD'
        group by n_nationkey, n_name
        order by n_nationkey asc;
     */

    val nationMapping =
      sc.broadcast(
        nation
        .flatMap(line => {
          val tokens = line.split("\\|")
          // (nationkey, nation_name)
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
        // (orderkey, custkey)
        List((tokens.head, tokens(1)))
      })
      .map((m) => (m._1, m._2))
      .cogroup(
        lineitem
          .flatMap(line => {
            val tokens:Array[String] = line.split("\\|")
            val shipDate = tokens(shipDateColumn)
            if (checkDate(date, shipDate)) {
              // (orderkey)
              List((tokens.head, shipDate))
            } else {
              List()
            }
          })
          // (orderkey, 1)
          .map((m) => (m._1, m._2))
      )
      .filter((f) => f._2._2.toList.nonEmpty)
      .map((m) => {
        val nationkey = customerMapping.value.get(m._2._1.toList.head).get
        ((nationkey, nationMapping.value.get(nationkey).get) , 1)
      })
      .reduceByKey(_+_)
      .repartitionAndSortWithinPartitions(myPartitioner)
      .sortBy((f) => f._1._1.toInt)
      .collect()
      .foreach((f) => {
        println("(" + f._1._1 + "," + f._1._2 + "," + f._2 + ")")
      })
  }
}