class Point (var line: String) extends Serializable{

  private val cols = line.split(",").map(_.trim())
  val values = cols.toList.map(_.toFloat)
  
  override def toString: String = {
    val str="Element : "+values.toString()+""
    str
  }

}

case class Cell(indexes: List[Int]) extends Serializable

case class PointInCell(point:Point, cell:Cell) extends Serializable
