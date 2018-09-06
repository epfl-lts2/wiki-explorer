/**
  * Created by volodymyrmiz on 16/08/18.
  */

import java.net.URLConnection
import java.util.Calendar

import ch.epfl.lts2.Utils.suppressLogs
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.array

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.parsing.json.JSON

object Tests extends App {

  suppressLogs(List("org", "akka"))

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Test")
    .getOrCreate()

//  val lines = spark.sparkContext.parallelize(
//    Seq("Spark Intellij Idea Scala test one",
//      "Spark Intellij Idea Scala test two",
//      "Spark Intellij Idea Scala test three"))
//
//  val counts = lines
//    .flatMap(line => line.split(" "))
//    .map(word => (word, 1))
//    .reduceByKey(_ + _)
//
//  counts.foreach(println)

//  import spark.implicits._
//  val left = Seq((0, "zero"), (1, "one")).toDF("page", "dailyTotal")
//  val right = Seq((0, "zero"), (2, "two"), (3, "three")).toDF("page", "dailyTotal")
//
//  val df = right.withColumnRenamed("dailyTotal", "dt1").join(left.withColumnRenamed("dailyTotal", "dt2"), Seq("page"), "fullouter").select($"*", array($"dt1", $"dt2"))
//
//  df.show()

  println(Calendar.getInstance().getTime())

val df = spark.read.load("jan18.parquet")
  df.show()
  println(df.count())
//
    import org.apache.spark.sql.functions.udf

  val TITLE1 = ".net"
  val TITLE2 = "Albert+Einstein"



  import scala.io.Source._
  import org.json4s.jackson.JsonMethods.parse

  def get(url: String,
          connectTimeout: Int = 100000,
          readTimeout: Int = 100000,
          requestMethod: String = "GET"): String =
  {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
    }

  def getLinks(title: String): List[String] = {
//    println(title)
    val URL = "https://en.wikipedia.org/w/api.php?action=query&titles=" + title + "&prop=links&pllimit=500&format=json"

    // define nested types of a Wikipedia API response
    type linksType = Map[String, Any]
    type pagesIDType = Map[String, linksType]
    type pagesType = Map[String, pagesIDType]
    type responseType = Map[String, pagesType]

    import scala.collection.mutable.ListBuffer
    var titles = new ListBuffer[String]()

      var responseJSON: responseType = parse(get(URL))
      .values
      .asInstanceOf[responseType]

//    println(responseJSON)

    var keys: Set[String] = responseJSON.keys.toSet

    if (keys.contains("query")) {

      val pageID = responseJSON("query")("pages").keys.head.toString

      var pageKeys = responseJSON("query")("pages")(pageID).keys.toSet

      if (pageKeys.contains("links")) {
        while (keys.contains("continue")) {
          responseJSON("query")("pages")(pageID)("links").asInstanceOf[List[Map[String, Any]]].filter(v => v("ns") == 0).map(_ ("title"))
            .map(v => titles += v.toString)
          //        println(responseJSON)
          responseJSON = parse(get(URL + "&plcontinue=" + responseJSON("continue")("plcontinue")).mkString)
            .values
            .asInstanceOf[responseType]

          //        println(responseJSON)
          keys = responseJSON.keys.toSet
        }

        responseJSON("query")("pages")(pageID)("links").asInstanceOf[List[Map[String, Any]]].filter(v => v("ns") == 0).map(_ ("title"))
          .map(v => titles += v.toString)
        //      println(responseJSON)
      }
    }
    titles.toList
  }

  println(getLinks(TITLE1))

  def getLinksUDF =
    udf((x: String) => {
      getLinks(x)
    })

  val dfLinks = df.withColumn("links", getLinksUDF.apply(df("page")))

  dfLinks
    .write.save("jan18Links.parquet")

  println(Calendar.getInstance().getTime())
}