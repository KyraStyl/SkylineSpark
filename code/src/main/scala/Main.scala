import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer

object Main extends App {

  // Create spark configuration
  val sparkConf = new SparkConf()
    .setMaster("local[8]")
    .setAppName("Skyline")
  val spark =SparkSession.builder().config(sparkConf).getOrCreate()
  val sc=spark.sparkContext
  Logger.getLogger("org").setLevel(Level.OFF)

  if (args.length == 0) {
    println("No arguments passed !")
  } else {
    try {
      val filename = args(0)
      val query = args(1)

      var k = 3
      if (args.length == 3) {
        k = args(2).toInt
      }
      println(filename, query, k)

      println("Reading from input file : " + filename + " . . .")

      val points = sc.textFile(filename).map(line => new Point(line))
      println(points.count() + " elements loaded.")

      val perc = 0.02
      val number_of_cells = 10 //per dimension
      val sample = points.sample(false, perc)
      println("Sample size = " + sample.count)

      // for every dimension
      // get elements and sort, then get the elements in n positions
      var cell = new ListBuffer[List[Float]]()
      for (dimension <- sample.first().values.indices) {
        val sort = sample.map(x => x.values(dimension)).collect().sorted
        var list = new ListBuffer[Float]()
        for (i <- 0 until number_of_cells - 1) {
          val position = (i / number_of_cells.toFloat) * sort.length
          list += sort(position.toInt)
        }
        cell += list.toList
      }
      println("Boundaries per dimension")
      cell.toList.foreach(println)
      // next broadcast cells and map all elements to each of these cells
      val cellBounds = sc.broadcast(cell.toList)

      //map each point to a cell
      val mapToCells2: RDD[PointInCell] = points.map(point => {
        val bounds = cellBounds.value
        val cells: ListBuffer[Int] = ListBuffer.fill(bounds.size)(-1)
        for (dimension <- bounds.indices) { //looping through dimensions
          for (bounds_index <- bounds(dimension).indices) {
            if (point.values(dimension) < bounds(dimension)(bounds_index) && cells(dimension) == -1) {
              cells(dimension) = bounds_index
            }
          }
          if (cells(dimension) == -1) {
            cells(dimension) = bounds(dimension).size
          }
        }
        PointInCell(point, Cell(cells.toList))
      })

      println("An example of  how PointInCell is represented")
      mapToCells2.take(1).foreach(x => println(x.point, x.cell))
      val mapToCells=mapToCells2.repartition(8)

      if (query == "skyline") { //This is the code for skyline
        spark.time({
          val skyline = Skyline.computeSkyline(mapToCells)
          println("Points in skyline: ")
          skyline.foreach(println)
        })
      } else if (query == "topk") { //This is the code for top k
        spark.time({
          println("Top " + k + " points")
          TOPk.computeTopk(mapToCells, k).foreach(println)
        })
      } else if (query == "skyline_topk") { //This is the code for topk of skyline
        spark.time({
          println("Top " + k + " points of the skyline")
          TopkSkyline.calculateTopKSkyline(mapToCells, k).foreach(println)
        })
      }

    } catch {
      case _: java.io.FileNotFoundException => println("This file could not be found!")
    }
  }


  sc.stop()
}
