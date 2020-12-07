class Point (var line: String){

  private val cols = line.split(",").map(_.trim())
  val values = cols.toList

}