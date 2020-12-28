object Utils {

  def isPointDominated(pointA: Point,pointB: Point): Boolean ={
    val a = pointA.values
    val b = pointB.values
    for (i <- a.indices){
      if(a(i).toFloat<b(i).toFloat) return false
    }
    true
  }

  def isCellDominated(cellA: Cell, cellB: Cell): Boolean ={
    val a = cellA.indexes
    val b = cellB.indexes
    for (i <- a.indices){
      if(a(i)<=b(i)) return false
    }
    true
  }
}
