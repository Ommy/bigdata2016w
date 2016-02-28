package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{DateChecker, Conf}
import org.apache.spark.{SparkContext, SparkConf}

object Q3 extends DateChecker{

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl", args.reducers())
    val parts = sc.textFile(args.input() + "/part.tbl", args.reducers())
    val supplier = sc.textFile(args.input() + "/supplier.tbl", args.reducers())
    val shipDateColumn = 10
    val partKeyColumn = 1
    val suppKeyColumn = 2
    val orderKeyColumn = 0
    val date = args.date()

    val partsMapping =
      sc.broadcast(parts
        .map(line => {
          val tokens = line.split("\\|")
          // partkey, name
          (tokens(0), tokens(1))
        })
        .collectAsMap())

    val supplierMapping =
      sc.broadcast(supplier
        .map(line => {
          val tokens = line.split("\\|")
          // supkey, name
          (tokens(0), tokens(1))
        })
        .collectAsMap())

    lineitem
      .flatMap(line => {
        val tokens = line.split("\\|")
        val shipDate = tokens(shipDateColumn)
        if (checkDate(date, shipDate)) {
          val partKey = tokens(partKeyColumn)
          val suppKey = tokens(suppKeyColumn)
          List((supplierMapping.value.get(suppKey).get, partsMapping.value.get(partKey).get, tokens(orderKeyColumn)))
        } else {
          List()
        }
      })
      .map((m) => (m._3, (m._2, m._1)))
      .sortBy(f => f._1.toInt)
      .take(20)
      .foreach((f) => {
        println("(" + f._1 + "," + f._2._1 + "," + f._2._2 + ")")
      })

  }

}