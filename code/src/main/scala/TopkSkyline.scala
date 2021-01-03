import TOPk.{calculateMetricsRDD, scorePoint}
import Utils.{calculateCellsThatPartiallyDominates, isPointDominated}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TopkSkyline {

  def calculateTopKSkyline(points: RDD[PointInCell], k: Int): Array[(PointInCell, Int)] = {
    val spark = SparkSession.builder().getOrCreate()
    val skyline = spark.sparkContext.parallelize(Skyline.computeSkyline(points).toSeq)
    val partByCellRDD = points.map(x => (x.cell, 1)).reduceByKey(_ + _)
    val metricsRDD = calculateMetricsRDD(partByCellRDD)
    val metrics = metricsRDD.map(x=>((x.cell,x.size),x.tl,x.tu,x.gf)).collect().toBuffer
//    val partByCell = points.map(x => (x.cell, 1)).reduceByKey(_ + _).collect().toBuffer
//    val metrics = calculateLowerUpperF(partByCell)
    scorePoint(skyline, k, metrics, points)

  }


}
