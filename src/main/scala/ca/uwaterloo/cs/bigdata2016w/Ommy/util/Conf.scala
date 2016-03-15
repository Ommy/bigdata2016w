package ca.uwaterloo.cs.bigdata2016w.Ommy.util

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = false)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
  val date = opt[String](descr = "date for query", required = false)
  val model = opt[String](descr = "model for query", required = false)
}
