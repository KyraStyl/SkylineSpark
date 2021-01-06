import Utils.{calculateCellsThatPartiallyDominates, isCellFullyDominated, isCellFullyDominated2, isCellPartiallyDominated, isCellPartiallyDominated2, isPointDominated}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TOPk {

  case class Metric(cell: Cell, size: Int, tl: Int, tu: Int, gf: Int)

  def computeTopk(points: RDD[PointInCell], k: Int): Array[(PointInCell, Int)] = {
    val spark = SparkSession.builder().getOrCreate()
    val partByCellRDD = points.map(x => (x.cell, 1)).reduceByKey(_ + _)
    val metricsRDD = calculateMetricsRDD(partByCellRDD)
    val c = calculateC(metricsRDD,k)
    val prunedCells=pruneCellsRDD(metricsRDD,k,c)
    val metrics = metricsRDD.map(x=>((x.cell,x.size),x.tl,x.tu,x.gf)).collect().toBuffer
    val broadcastPrunedCells = spark.sparkContext.broadcast(prunedCells)
    val prunedRDD = points.filter(x => broadcastPrunedCells.value.contains(x.cell))
    val pruneIncell = prunedRDD.groupBy(_.cell).map(x => x._2).map(x => pruneInCell(x, k)).flatMap(_.map(x => x))
    val totalElements = pruneIncell.count()
    println("We will consider: " + totalElements + " points")
    val data = scorePoint(pruneIncell, k, metrics, points)
    data

  }



  def calculateMetricsRDD(cells: RDD[(Cell, Int)]): RDD[Metric] = {
    cells.cartesian(cells)
      .map(x => {
        var tl = 0
        var tu = 0
        var gf = 0
        if (isCellPartiallyDominated(x._2._1, x._1._1)) tu += x._2._2
        if (x._1._1 != x._2._1 && isCellFullyDominated(x._2._1, x._1._1)) tl += x._2._2
        if (x._1._1 != x._2._1 && isCellFullyDominated(x._1._1, x._2._1)) gf += x._2._2
        Metric(x._1._1, x._1._2, tl, tu, gf)
      })
      .groupBy(_.cell)
      .map(x => {
        val elements = x._2.head.size
        var tl = 0
        var tu = 0
        var gf = 0
        x._2.foreach(m => {
          tl += m.tl
          tu += m.tu
          gf += m.gf
        })
        Metric(x._1, elements, tl, tu, gf)
      })
  }

  def calculateC(metrics:RDD[Metric],k:Int):Int={
    val ms = metrics
      .sortBy(_.tl,ascending = false)
      .take(k)
    var m = 0
    for (i<-ms){
      m+=i.tl
      if(m>=k) return i.tl
    }
    -1
  }

  def pruneCellsRDD(metrics:RDD[Metric],k:Int,c:Int):Array[Cell]={
    metrics.filter(m=> m.gf<k && m.tu>=c).map(_.cell).collect()
  }
  def pruneInCell(points: Iterable[PointInCell], k: Int): ArrayBuffer[PointInCell] = {
    var pr = points.toBuffer
    val toKeep = new ArrayBuffer[PointInCell]()
    for (i <- pr.indices) {
      var m = 0
      for (j <- pr.indices) {
        if (i != j && isPointDominated(pr(i).point, pr(j).point)) m += 1
      }
      if (m <= k) toKeep.append(pr(i))
    }
    toKeep
  }

  def scorePoint(skyline: RDD[PointInCell], k: Int, metrics: mutable.Buffer[((Cell, Int), Int, Int, Int)], rddPoints: RDD[PointInCell]): Array[(PointInCell,Int)] = {
    val spark = SparkSession.builder().getOrCreate()
    val cells: Array[Cell] = rddPoints.groupBy(x => x.cell).map(x => x._1).collect()
    val broadcastMetrics = spark.sparkContext.broadcast(metrics)
    val broadcastCells = spark.sparkContext.broadcast(cells)
    val rddSkyline:RDD[(PointInCell,(Int,ArrayBuffer[Cell]))] = skyline.map(skylinePoint => {
      val tl = broadcastMetrics.value.filter(x => x._1._1 == skylinePoint.cell).head._2
      val pcells = calculateCellsThatPartiallyDominates(skylinePoint.cell, broadcastCells.value)
      (skylinePoint ,(tl, pcells))
    }).persist(StorageLevel.MEMORY_AND_DISK)
    val extraPoints = rddSkyline.cartesian(rddPoints)
      .filter(combination => {
        combination._1._2._2.contains(combination._2.cell)
      })
      .map(combination => {
        var dominated = 0
        if (isPointDominated(combination._2.point, combination._1._1.point)) dominated = 1
        (combination._1._1.point.toString,(combination._1._1, dominated))
      })
      .reduceByKey((x,y)=>{
        (x._1,x._2+y._2)
      })
    val score = rddSkyline.map(x=>{
      (x._1.point.toString,x._2._1)
    })
      .join(extraPoints)
      .map(x=>{
        (x._2._2._1,x._2._1+x._2._2._2)
      })
      .collect()
      .sortWith((x,y)=>x._2>y._2)
      .slice(0,k)

    rddSkyline.unpersist()
    score
  }


}