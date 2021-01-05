class Point (var line: String) extends Serializable{

  private val cols = line.split(",").map(_.trim())
  val values = cols.toList.map(_.toFloat)
  
  override def toString: String = {
    val str="Element : "+values.toString()+""
    str
  }

   def equals(other: Point): Boolean = {
    val othersValues = other.values
    for (i <- values.indices){
      if (values(i)!=othersValues(i)) return false
    }
    true
  }

}

case class Cell(indexes: List[Int]) extends Serializable{
  override def toString: String = {
    var str = indexes.take(1).head.toString
    var p=0
    for (i <- indexes){
      if (p >0){
        str += ","+i.toString
      }
      p+=1
    }
    str
  }
}

case class PointInCell(point:Point, cell:Cell) extends Serializable

