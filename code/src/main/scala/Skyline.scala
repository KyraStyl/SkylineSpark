import Utils.{isCellFullyDominated, isPointDominated}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Skyline {

  def computeSkyline(points:RDD[PointInCell]): Iterable[PointInCell]={
    val partByCell = points.groupBy(x=>x.cell)
    val prunedCells = pruneDominatedCells(partByCell.map(x=>x._1).collect().toBuffer)

    val prunedRDD = partByCell.filter(x=>prunedCells.contains(x._1)).map(x=>(x._1,pruneDominatedPoints(x._2)))

//    val skyline = pruneDominatedPoints(prunedRDD.flatMap(x=>x._2).collect().toBuffer).map(x=>x.point)
    val skyline = pruneDominatedPoints(prunedRDD.flatMap(x=>x._2).collect().toBuffer)
    skyline

  }

  def pruneDominatedPoints(points: Iterable[PointInCell]): Iterable[PointInCell] ={
    var pr = points.toBuffer
    val toPrune = new ArrayBuffer[Int]()
    for(i <- pr.indices)
      for(j <- pr.indices){
        if(i!=j && isPointDominated(pr(j).point,pr(i).point)){
          toPrune.append(j)
        }
      }
    pr = pr.zipWithIndex.filter(x=> !toPrune.contains(x._2)).map(x => x._1)
    pr
  }


  def pruneDominatedCells(input:mutable.Buffer[Cell]):  mutable.Buffer[Cell] ={
    var pr = input
    val toPrune = new ArrayBuffer[Int]()
    for(i <- pr.indices)
      for(j <- pr.indices){
        if(i!=j && isCellFullyDominated(pr(j),pr(i))){
          toPrune.append(j)
        }
      }
    pr = pr.zipWithIndex.filter(x=> !toPrune.contains(x._2)).map(x => x._1)
    pr
  }

}
