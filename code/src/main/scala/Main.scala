import breeze.linalg.shuffle
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random
import org.apache.log4j.{Level, Logger}

object Main extends App {

  // Create spark configuration
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Skyline")

  // Create spark context
  val sc = new SparkContext(sparkConf)
  Logger.getLogger("org").setLevel(Level.OFF)
//  sc.setLogLevel("OFF")

  if (args.length == 0) {
    println("No arguments passed !")
  }else{
    try{
      val filename = args(0)

      println("Reading from input file : " + filename + " . . .")

      val points = sc.textFile(filename).map(line => new Point(line))
      println(points.count()+" elements loaded.")

      val perc = 0.35
      val sample = points.sample(false,perc)
      println("Sample size = "+sample.count)


    }catch {
      case _: java.io.FileNotFoundException => println("This file could not be found!")
    }
  }
  sc.stop()
}
