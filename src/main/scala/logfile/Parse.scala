package logfile

import com.typesafe.config.Config

import scala.io._

class Parse(logPath: String, logFormat: String, configFile: Config) {
  val lines = Source.fromFile(logPath).getLines.toList

  def ParseLog() = {
    val delimiter = configFile.getString("kugsha.profiles.logfile.delimiter")
    val userIdPosition = configFile.getInt("kugsha.profiles.logfile.userIdPosition")
    val timestampPosition = configFile.getInt("kugsha.profiles.logfile.timestampPosition")
    val urlPosition = configFile.getInt("kugsha.profiles.logfile.urlPosition")

    val result: List[List[String]] = lines.flatMap { line: String =>
      val lineSplited: List[String] = line.split(delimiter).toList
      // userId, timestamp, url
      List(lineSplited.take(userIdPosition), lineSplited.take(timestampPosition), lineSplited.take(urlPosition))
    }
  }
}
