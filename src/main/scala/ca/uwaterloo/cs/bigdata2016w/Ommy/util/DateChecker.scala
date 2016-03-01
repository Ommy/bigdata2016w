package ca.uwaterloo.cs.bigdata2016w.Ommy.util

trait DateChecker {

  def checkDate(date:String, otherDate:String): Boolean = {
    if (date.contains("-")) {
      val splitDate = date.split("-")
      val otherSplit = otherDate.split("-")
      for (i <- 0 to splitDate.size-1) {
        if (!splitDate(i).equals(otherSplit(i))) {
          return false
        }
      }
      true
    } else {
      otherDate.split("-").head.equals(date)
    }
  }

}
