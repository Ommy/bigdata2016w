package ca.uwaterloo.cs.bigdata2016w.Ommy.assignment5

import ca.uwaterloo.cs.bigdata2016w.Ommy.util.{DateChecker, Conf}
import org.apache.spark.{SparkContext, SparkConf}

object Q1 extends DateChecker {

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)
    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args.input() + "/lineitem.tbl", 10)
    val shipDateColumn = 10
    val date = args.date()

    println("ANSWER=" + textFile
      .flatMap(line => {
        val shipDate = line.split("\\|")(shipDateColumn)
        if (checkDate(date, shipDate)) {
          List(shipDate)
        } else {
          List()
        }
      })
      .map(m => (m, 1))
      .count())

  }

}