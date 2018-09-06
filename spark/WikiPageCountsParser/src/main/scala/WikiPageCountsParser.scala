/**
  * Created by volodymyrmiz on 16/08/18.
  */

import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, typedLit, udf}
import scala.reflect.runtime.universe.{TypeTag}
import ch.epfl.lts2.Utils._
import ch.epfl.lts2.Globals._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable


object WikiPageCountsParser extends App {

  suppressLogs(List("org", "akka"))

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Wiki PageCounts Parser")
    .config("spark.driver.maxResultSize", "20g") // change this if needed
    .config("spark.executor.memory", "50g") // change this if needed
    .getOrCreate()

  import spark.implicits._

  val SQLContext = spark.sqlContext
  val sc = spark.sparkContext

  // Choose files with page counts. Configure file names
  val YEAR = "2018-"
  val MONTH = "07-"
  val DAYS = 31 // number of days in a parsed month

  // Configure parser. Chose a Wikimedia project and threshold
  val PROJECT = "en.z" // Wikipedia prject code
  val DAILY_THRESHOLD = 100

  // Configure output
  val FOLDER = "jul18" // folder name for parsed output

  // PARSE
  log.info("Start time: " + Calendar.getInstance().getTime())

  // Initial record format
  case class Record(project: String, page: String, dailyTotal: Int, hourlyCounts: String)

  // Resulting record format. Added a column for the dates
  case class RecordDF(project: String, page: String, dailyTotal: Int, hourlyCounts: String, day: String)

  var df = spark.emptyDataset[RecordDF].toDF()

  // Read files in the resources folder one by one. Specify YEAR, MONTH, and DAYS (number of days in the month)
  // according to the file names.
  for (i <- 1 to DAYS) {
    var day = i.toString
    if (i <= 9) day = "0" + i.toString

    var t = sc.textFile(PATH_RESOURCES + FOLDER + "/pagecounts-" + YEAR + MONTH + day + ".bz2")
      .filter(line => !line.contains("#"))
      .map(_.split(" "))
      .map {
        case Array(project, page, dailyTotal, hourlyCounts) => Record(project, page, dailyTotal.toInt, hourlyCounts)
      }
      .toDF()
      .filter($"dailyTotal" > DAILY_THRESHOLD)
      .filter($"project" === PROJECT)
    t = t.withColumn("day", typedLit[String](YEAR + MONTH + day))
    df = df.union(t)
  }

  df = df.drop($"project")

  /***
    * User-Defined Function (UDF)
    * Takes two columns with types WrappedArray[TYPE1], WrappedArray[TYPE2]
    * and returns a Map corresponding to a zip of two arrays.
    * @tparam S type of items in WrappedArray in the first column
    * @tparam T type of items in WrappedArray in the second column
    * @return Map corresponding to a zip of two arrays Map(SECOND[i] -> FIRST[i]) with values of the second as keys.
    */
  private def toMapUDF[S: TypeTag, T: TypeTag] =
    udf((x: mutable.WrappedArray[S], y: mutable.WrappedArray[T]) => {
      val zipped = y zip x
      zipped.toMap
    })

  // collect daily visits per page
 var dfTS = df.groupBy($"page")
    .agg(collect_list($"dailyTotal"), collect_list($"day")) // get lists of visits and days for each page
    .toDF(Seq("page", "dailyTotal", "day"): _*) // rename columns

  // convert lists to time-series
  dfTS = dfTS
    .withColumn("ts", toMapUDF[Int, String].apply(dfTS("dailyTotal"), dfTS("day"))) // transform separate lists of dates and dailyCounts into a Map(day->visits)

  // save DataFrame with time-series
  dfTS.select("page", "ts")
    .write.save(PATH_RESOURCES + FOLDER + ".parquet")

  log.info("End time: " + Calendar.getInstance().getTime())
}
