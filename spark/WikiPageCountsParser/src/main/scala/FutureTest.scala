import ch.epfl.lts2.Utils.suppressLogs
import com.twitter.chill.Base64.InputStream
import org.apache.spark.sql.SparkSession

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Random, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import org.json4s.jackson.JsonMethods.parse

import scala.collection.mutable.ListBuffer

/**
  * Created by volodymyrmiz on 21/08/18.
  */
object FutureTest extends App{
  suppressLogs(List("org", "akka"))

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Future Test")
    .getOrCreate()

  def get(url: String,
          connectTimeout: Int = 100000,
          readTimeout: Int = 100000,
          requestMethod: String = "GET"): Future[String] =
  {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)

    Future {
      val inputStream = connection.getInputStream
      val content = scala.io.Source.fromInputStream(inputStream).mkString
      if (inputStream != null) inputStream.close
      content
    }
  }

  val TITLE1 = ".net"
  val TITLE2 = "Albert+Einstein"

  val URL = "https://en.wikipedia.org/w/api.php?action=query&titles=" + TITLE1 + "&prop=links&pllimit=500&format=json"

  import akka.actor.ActorSystem
  import akka.pattern.Patterns.after
  import scala.concurrent.duration._

  // retry feature if it fails
  def retry(future: Future[String], factor: Float = 1.5f, init: Int = 1000, cur: Int = 0)(implicit as: ActorSystem): Future[String] = {
    future.recoverWith {
      case e: java.io.IOException =>
        val next: Int =
          if (cur == 0) init
          else Math.ceil(cur * factor).toInt
        println(s"retrying after ${next} ms")
        after(next.milliseconds, as.scheduler, global, Future.successful(1)).flatMap { _ => retry(future)}
      case t: Throwable => throw t
    }
  }

  val as = ActorSystem()

  def getLinks(title: String): List[String] = {
        println(title)
    val URL = "https://en.wikipedia.org/w/api.php?action=query&titles=" + title + "&prop=links&pllimit=500&format=json"

    // define nested types of a Wikipedia API response
    type linksType = Map[String, Any]
    type pagesIDType = Map[String, linksType]
    type pagesType = Map[String, pagesIDType]
    type responseType = Map[String, pagesType]

    import scala.collection.mutable.ListBuffer
    var titles = new ListBuffer[String]()

    var responseFuture = retry(get(URL))(as)

    var r = Await.result(responseFuture, 1000 minutes)

    var responseJSON: responseType = parse(r)
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

          responseFuture = retry(get(URL + "&plcontinue=" + responseJSON("continue")("plcontinue")))(as)
          r = Await.result(responseFuture, 1000 minutes)
          responseJSON = parse(r)
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

//  println(getLinks(TITLE2))

  val df = spark.read.load("jan18.parquet")
  df.show()
  println(df.count())

  import org.apache.spark.sql.functions.udf

    def getLinksUDF =
      udf((x: String) => {
        getLinks(x)
      })

    val dfLinks = df.withColumn("links", getLinksUDF.apply(df("page")))

    dfLinks
      .write.save("jan18Links.parquet")
}
