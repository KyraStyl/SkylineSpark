import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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
  } else {
    try {
//      val filename = args(0)
      val filename= "dataset1.csv"
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
      println("Boundaries per dimension")
      cell.toList.foreach(println)
      // next broadcast cells and map all elements to each of these cells
      val cellBounds=sc.broadcast(cell.toList)

      //map each point to a cell
      val mapToCells:RDD[PointInCell]=points.map(point=>{
        val bounds=cellBounds.value
        var cells :ListBuffer[Int] = ListBuffer.fill(bounds.size)(-1)
        for(dimension <- bounds.indices){ //looping through dimensions
          for (bounds_index <- bounds(dimension).indices){
            if (point.values(dimension)<bounds(dimension)(bounds_index) && cells(dimension)== -1){
              cells(dimension)=bounds_index
            }
          }
          if(cells(dimension) == -1){
            cells(dimension)=bounds(dimension).size
          }
        }
        PointInCell(point,Cell(cells.toList))
      })
      //Its working
      println("An example of  how PointInCell is represented")
      mapToCells.take(1).foreach(x=>println(x.point,x.cell))

      //This is the code for skyline
      val skyline= Skyline.computeSkyline(mapToCells)
      println("Points in skyline: ")
      skyline.foreach(println)


    } catch {
      case _: java.io.FileNotFoundException => println("This file could not be found!")
    }
  }




  sc.stop()
}
