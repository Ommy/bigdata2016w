package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{Conf, DateChecker}
import org.apache.spark.{SparkContext, SparkConf}


object Q6 extends DateChecker {


  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    val lineitem = sc.textFile(args.input() + "/lineitem.tbl", 10)
    val date = args.date()

    /**
     *
      select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
        sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
      from lineitem
      where
        l_shipdate = 'YYYY-MM-DD'
      group by l_returnflag, l_linestatus;
     */
    val quantityIdx       = 4
    val extendedPriceIdx  = 5
    val discountIdx       = 6
    val taxIdx            = 7
    val returnflagIdx     = 8
    val linestatusIdx     = 9
    val shipDateIdx       = 10

    /*
      Return flag: R / A at random if receiptdate <= currentdate
                   else N
      Line Status: O if shipdate > currentdate
                   F otherwise
     */

    lineitem
      .flatMap(line => {
        val t = line.split("\\|")
        if (checkDate(date, t(shipDateIdx))) {
          val ep:Double = t(extendedPriceIdx).toDouble
          val dc:Double = t(discountIdx).toDouble
          val tx:Double = t(taxIdx).toDouble
          val q:Double = t(quantityIdx).toDouble

          List(((t(returnflagIdx), t(linestatusIdx)), (q, ep, ep*(1-dc), ep*(1-dc)*(1+tx), q, ep, dc)))
        } else {
          List()
        }
      })
      .mapPartitions((m) => {
        m.map(f => {
          (f._1, (f._2, 1))
        })
      })
      .reduceByKey((x, y) => {
        ((x._1._1 + y._1._1, x._1._2 + y._1._2, x._1._3 + y._1._3, x._1._4 + y._1._4, x._1._5 + y._1._5, x._1._6 + y._1._6, x._1._7 + y._1._7), x._2 + y._2)
      })
      .map((m) => {
        (m._1, ((m._2._1._1, m._2._1._2, m._2._1._3, m._2._1._4, m._2._1._5/ m._2._2, m._2._1._6 / m._2._2, m._2._1._7 / m._2._2), m._2._2))
      })
      .collect()
      .foreach(f => {
        println(f._1 + " -- " + f._2._1 + " -- " + f._2._2)
      })

  }
}
