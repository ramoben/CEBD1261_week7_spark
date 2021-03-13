import org.apache.log4j._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.reflect.internal.util.StringOps.words

object wordCount_a {
  def main(args: Array[String]) {
    val filePath: String = "TheHungerGames_txt.txt"

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName("word_1")
      .getOrCreate()
    val input = session.read.textFile(filePath)
    import session.implicits._
    //val stopWords = ("the","i","to", "and", "a", "of", "me", "my", "we", "our", "you", "in", "it", "but", "that")


    val wordCount = input.flatMap(x => x.split("\\W+"))
      .filter(m => m.matches("\\w*[a-zA-Z]\\w*"))
      .map(w => w.toLowerCase())
      .filter(m=>m!="the")
      .filter(m=>m!="i")
      .filter(m=>m!="to")
      .rdd.countByValue().toSeq.sortBy(_._2).reverse

    wordCount.foreach(println)
  }
}
