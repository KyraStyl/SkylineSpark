import breeze.linalg.shuffle
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
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
  //  Logger.getLogger("org").setLevel(Level.OFF)
  sc.setLogLevel("OFF")

  if (args.length == 0) {
    println("No arguments passed !")
  } else {
    try {
      val filename = args(0)

      println("Reading from input file : " + filename + " . . .")

      val points = sc.textFile(filename).map(line => new Point(line))
      println(points.count() + " elements loaded.")

      val perc = 0.10 // TODO: check this so it will be able to load them to drivers memory
      val number_of_cells = 10
      val sample = points.sample(false, perc)
      println("Sample size = " + sample.count)

      // for every dimension
      // get elements and sort, then get the elements in n positions
      var cell = new ListBuffer[List[Float]]()
      for (dimension <- sample.first().values.indices) {
        val sort = sample.map(x => x.values(dimension)).collect().sorted
        var list = new ListBuffer[Float]()
        for (i <- 0 until number_of_cells) {
          val position = (i / number_of_cells.toFloat) * sort.length
          list += sort(position.toInt)
        }
        cell += list.toList
      }

      // next broadcast cells and map all elements to each of these cells




    } catch {
      case _: java.io.FileNotFoundException => println("This file could not be found!")
    }
  }
  sc.stop()
}
