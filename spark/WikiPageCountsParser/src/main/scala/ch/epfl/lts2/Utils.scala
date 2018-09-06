package ch.epfl.lts2

/**
  * Created by volodymyrmiz on 16/08/18.
  */
package object Utils {
  def suppressLogs(params: List[String]): Unit = {
    import org.apache.log4j.{Level, Logger}
    params.foreach(Logger.getLogger(_).setLevel(Level.OFF))
  }
}
