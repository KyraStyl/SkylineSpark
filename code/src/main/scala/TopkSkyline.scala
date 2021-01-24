import TOPk.{calculateMetricsRDD, scorePoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object TopkSkyline {

  def calculateTopKSkyline(points: RDD[PointInCell], k: Int): Array[(PointInCell, Int)] = {
    val spark = SparkSession.builder().getOrCreate()
    val skyline = spark.sparkContext.parallelize(Skyline.computeSkyline(points).toSeq)
    val partByCellRDD = points.map(x => (x.cell, 1)).reduceByKey(_ + _)
    val metricsRDD = calculateMetricsRDD(partByCellRDD)
    val metrics = metricsRDD.map(x=>((x.cell,x.size),x.tl,x.tu,x.gf)).collect().toBuffer
    scorePoint(skyline, k, metrics, points)

  }


}
