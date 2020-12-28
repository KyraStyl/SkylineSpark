import Utils.{isCellDominated, isCellFullyDominates, isCellPartiallyDominates, isPointDominated, isPointDominates}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TOPk {

  //Iterable[Point]
  def computeTopk(points: RDD[PointInCell], k: Int):List[(PointInCell,Long)] = {
    val partByCell = points.map(x=>(x.cell,1)).reduceByKey(_+_).collect().toBuffer //one pass to get count for every cell
    val metrics=calculateLowerUpperF(partByCell) //calculate tl, tu, gf
    val c=calculatec(metrics,k) //calculate γ
    val prunedCells=pruneCells(metrics,k,c) //prune based on the tl, gf and γ
    val prunedRDD =points.groupBy(x=>x.cell).filter(x=>prunedCells.contains(x._1))
    val data=scorePoint(prunedRDD.flatMap(x=>x._2).collect().toBuffer,k,metrics,points)
    print("hi")
    data

  }

  def calculateLowerUpperF(cellCounts:mutable.Buffer[(Cell,Int)]):ArrayBuffer[((Cell,Int),Int,Int,Int)] ={
    val list = new ArrayBuffer[((Cell,Int),Int,Int,Int)]()
    for( i <- cellCounts.indices){
      var tl=0
      var tu=0
      var gf=0
      for (j <- cellCounts.indices){
        if(Utils.isCellPartiallyDominates(cellCounts(i)._1,cellCounts(j)._1)) tu+=cellCounts(j)._2
        if(i!=j){
          if(Utils.isCellFullyDominates(cellCounts(i)._1,cellCounts(j)._1)) tl+=cellCounts(j)._2
          if(Utils.isCellDominated(cellCounts(i)._1,cellCounts(j)._1)) gf+=cellCounts(j)._2
        }
      }
      list.append((cellCounts(i),tl,tu,gf))
    }
    list
  }

  def calculatec(metrics:ArrayBuffer[((Cell,Int),Int,Int,Int)],k:Int): Int ={
    var m=0
    for (i <-metrics sortWith ((x, y) => x._2 > y._2)){
      m+=i._1._2
      if (m>=k) return i._2
    }
    -1
  }

  def pruneCells(metrics:ArrayBuffer[((Cell,Int),Int,Int,Int)],k:Int,c:Int): ArrayBuffer[Cell] ={
    var pr = metrics
    val toPrune = new ArrayBuffer[Int]()
    for(i <- pr.indices){
      if (pr(i)._4>=k){
        toPrune.append(i)
      }
      else if(pr(i)._3<c){
        toPrune.append(i)
      }
    }
    pr.zipWithIndex.filter(x=> !toPrune.contains(x._2)).map(x => x._1._1._1)

  }

  def pruneKDominatedPointsInCell(points: Iterable[PointInCell], k:Int):Iterable[(PointInCell,Int)]={
    var pr = points.toBuffer
    val toKeep = new ArrayBuffer[(Int,Int)]()
    for(i <- pr.indices) {
      var m=0
      var d=0
      for(j <- pr.indices){
        if(i!=j && isPointDominated(pr(j).point,pr(i).point))  m+=1
        if(i!=j && isPointDominates(pr(j).point,pr(i).point)) d+=1
      }
      if (m<k) toKeep.append((i,d))

    }
    toKeep.map(x=>(pr(x._1),x._2))

  }

  def scorePoint(points: Iterable[PointInCell],k:Int,metrics:ArrayBuffer[((Cell,Int),Int,Int,Int)],rddPoints:RDD[PointInCell]):List[(PointInCell,Long)]={
    var pr = points.toBuffer
    val score = new ArrayBuffer[(PointInCell,Long)]()
    val cells:Array[Cell]=rddPoints.groupBy(x=>x.cell).map(x=>x._1).collect()
    for (i <- pr.indices){
      val tl= metrics.filter(x=>x._1._1==pr(i).cell).head._2
      //for all the cells that partially dominates
      var pcells = calculateCellsThatPartiallyDominates(pr(i).cell,cells)
      val extraP=rddPoints.filter(x=>pcells.contains(x.cell)).map(x=>{
        if (isPointDominates(pr(i).point,x.point)) 1
        else 0
      }).sum().toLong
      score.append((pr(i),tl+extraP))
    }
    score.sortWith((x,y)=>x._2>y._2).toList.take(k)
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
      if (isLegit && !isCellFullyDominates(cell,acell)) partiallyDomCells.append(acell)
    }
    partiallyDomCells
  }

}
