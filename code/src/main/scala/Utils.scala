object Utils {

  def mayPointBeDominated(pointA: Point,pointB: Point): Boolean ={
    val a = pointA.values
    val b = pointB.values
    for (i <- a.indices){
      if(a(i)<b(i)) return false
    }
    true
  }

  def isPointDominated(pointA: Point, pointB: Point): Boolean ={
    mayPointBeDominated(pointA,pointB)&& !mayPointBeDominated(pointB,pointA)
  }

  def isCellPartiallyDominated(cellA: Cell, cellB: Cell): Boolean ={
    val a = cellA.indexes
    val b = cellB.indexes
    for (i <- a.indices){
      if(a(i)<b(i)) return false
    }
    true
  }

  def isCellFullyDominated(cellA: Cell, cellB: Cell): Boolean ={
    val a = cellA.indexes
    val b = cellB.indexes
    for (i <- a.indices){
      if(a(i)<=b(i)) return false
    }
    true
  }

}
