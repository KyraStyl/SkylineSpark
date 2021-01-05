import scala.collection.mutable.ArrayBuffer

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
    mayPointBeDominated(pointA,pointB) && !mayPointBeDominated(pointB,pointA)
  }

  def isCellPartiallyDominated(cellA: Cell, cellB: Cell): Boolean ={
    val a = cellA.indexes
    val b = cellB.indexes
    for (i <- a.indices){
      if(a(i)<b(i)) return false
    }
    true
  }

  def isCellPartiallyDominated2(cellA: Cell, cellB: Cell): Boolean ={
    val LBb = calcLB(cellB)
    val LBa = calcLB(cellA)
    LBb.equals(LBa) || isPointDominated(LBa,LBb)
  }

  def isCellFullyDominated(cellA: Cell, cellB: Cell): Boolean ={
    val a = cellA.indexes
    val b = cellB.indexes
    for (i <- a.indices){
      if(a(i)<=b(i)) return false
    }
    true
  }

  def isCellFullyDominated2(cellA: Cell, cellB: Cell): Boolean ={
    val UBb = calcUB(cellB)
    val LBa = calcLB(cellA)
    UBb.equals(LBa) || isPointDominated(LBa,UBb)
  }

  def calcUB(cell: Cell): Point ={
    val indexes = cell.indexes.map(x=>x+1)
    calcLB(Cell(indexes))
  }

  def calcLB(cell: Cell): Point ={
    new Point(cell.toString)
  }

  def calculateCellsThatPartiallyDominates(cell:Cell,cells:Array[Cell]): ArrayBuffer[Cell] ={
    val partiallyDomCells = new ArrayBuffer[Cell]()
    for(acell:Cell <- cells){
      var isLegit=true
      for (i<-cell.indexes.indices){
        if (cell.indexes(i)>acell.indexes(i)){
          isLegit=false
        }
      }
      if (isLegit && !isCellFullyDominated(acell,cell)) partiallyDomCells.append(acell)
    }
    partiallyDomCells
  }

}
