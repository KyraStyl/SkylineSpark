import TOPk.{calculateCellsThatPartiallyDominates, calculateLowerUpperF}
import Utils.isPointDominated
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object TopkSkyline {

  def calculateTopKSkyline(points: RDD[PointInCell], k: Int): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val skyline = spark.sparkContext.parallelize(Skyline.computeSkyline(points).toSeq)
    val partByCell = points.map(x => (x.cell, 1)).reduceByKey(_ + _).collect().toBuffer
    val metrics = calculateLowerUpperF(partByCell)
    val data = scorePoint(skyline, k, metrics, points)
  }

  def scorePoint(skyline: RDD[PointInCell], k: Int, metrics: ArrayBuffer[((Cell, Int), Int, Int, Int)], rddPoints: RDD[PointInCell]): Array[(PointInCell,Int)] = {
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
      .sortBy(x=>x._2,ascending = false)
      .take(k)
    rddSkyline.unpersist()
    score


  }

}
