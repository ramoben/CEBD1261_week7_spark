import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.log4j.{Level, Logger}

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
/** Find the movies with the most ratings. */
object Popular_1 {

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named PopularMovies
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies")
    val sc = new SparkContext(conf)

    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)

    // Read in each rating line
    val lines = sc.textFile("ml-100k/u.data")
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    // val movies = lines.map(x => (x.split("\t")(1).toInt, x.split("\t")(2).toInt))
    // Count up all the 1's for each movie
     val movieCounts = movies.reduceByKey( (x, y) => x + y )
    // val movieCounts = movies.reduceByKey( (x, y) => (x + y)/2)
    // val movieCounts = movies.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    //val movieCounts = movies.combineByKey(
    //  (v) => (v, 1),
    //  (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
    //  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    // ).map{ case (key, value) => (key, value._1 / value._2.toFloat) }
    //movies.collectAsMap().map(println(_))
    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    // Sort
    val sortedMovies = flipped.sortByKey()


    // Fold in the movie names from the broadcast variable
    val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))

    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    // results.foreach(println)
    //sortedMovies.foreach(println)
    results.foreach(println)
    //df2.select('number, '_1 as 'letter).repartition('number).sortWithinPartitions('number, 'letter).groupBy('number).agg(collect_list('letter)).show()

  }

}