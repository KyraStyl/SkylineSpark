import breeze.linalg.shuffle
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random


object Main extends App {

  // Create spark configuration
  val sparkConf = new SparkConf()
    .setMaster("local[8]")
    .setAppName("Skyline")

  // Create spark context
  val sc = new SparkContext(sparkConf)
  sc.setLogLevel("OFF")

  if (args.length == 0) {
    println("No arguments passed !")
  }else{
    try{
      val filename = args(0)

      val bufferedSource = Source.fromFile(filename)
      println("Reading from input file : " + filename + " . . .")

      val pointsArray = ArrayBuffer[Point]()

      for (line <- bufferedSource.getLines()) {
        pointsArray += new Point(line)
      }
      bufferedSource.close

      println(pointsArray.length+" elements loaded!")

      val perc = 0.35
      val sample = Random.shuffle(pointsArray.toList).take((perc*pointsArray.length).ceil.toInt)
      println("Sample size = "+sample.length)

      val points = sc.parallelize(pointsArray)


    }catch {
      case _: java.io.FileNotFoundException => println("This file could not be found!")
    }
  }
  sc.stop()
}