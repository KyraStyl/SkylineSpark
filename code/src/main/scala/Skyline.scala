import Utils.{calcLB, calcUB, isCellFullyDominated, isCellPartiallyDominated, isPointDominated}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Skyline {

  def computeSkyline(points:RDD[PointInCell]): Iterable[PointInCell]={
    val partByCell = points.groupBy(_.cell)
    val cellsrdd = partByCell.map(_._1)
    val cartesianrdd = cellsrdd.cartesian(cellsrdd)
    val remainingrdd = cartesianrdd
      .filter(x=> !x._1.equals(x._2))
      .map(x=> (x._1,isCellFullyDominated(x._1,x._2)))
      .groupBy(_._1)
      .filter(x=> !x._2.toList.contains((x._1,true)))
      .map(x=>x._1)
    val list=remainingrdd.collect()
    val prunedRDD = partByCell
      .filter(x=> list.contains(x._1))
      .map(x=>(x._1,pruneDominatedPoints(x._2)))
      .flatMap(x=>x._2)
    val skyline = pruneDominatedPoints(prunedRDD.collect())
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
}
