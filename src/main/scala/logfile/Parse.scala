package logfile

import com.fasterxml.jackson.databind.JsonNode
import com.netaporter.uri.Uri
import com.typesafe.config.Config
import com.netaporter.uri.dsl._
import org.bson.{ BsonArray, BsonString }
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.mutable.Document

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.collection.JavaConversions._
import database.Helpers._
import play.api.libs.json._

case class Profile(
  id: String,
  preferencesProbabilities: mutable.HashMap[String, Double],
  pageTypeProbabilities: mutable.HashMap[String, Double],
  visitedPages: mutable.ListBuffer[(mutable.ListBuffer[String], Long)],
  var firstTime: DateTime,
  averageTime: Long
)

case class LogEntry(id: String, timestamp: DateTime, url: String)

case class LogPageEntry(url: String, kind: String, category: String)

class Parse(configFile: Config, db: MongoDatabase, collectionName: String, isJSON: Boolean) {

  val users = mutable.HashMap[String, Profile]()
  val pages = mutable.HashMap[String, LogPageEntry]()

  def ParseLog() = {
    val delimiter = configFile.getString("kugsha.profiles.logfile.delimiter")
    val userIdPosition = configFile.getInt("kugsha.profiles.logfile.userIdPosition")
    val timestampPosition = configFile.getInt("kugsha.profiles.logfile.timestampPosition")
    val urlPosition = configFile.getInt("kugsha.profiles.logfile.urlPosition")
    val ignoreList = configFile.getStringList("kugsha.profiles.logfile.ignoreList").toList
    val logPath = configFile.getString("kugsha.profiles.logfile.path")
    val dateFormat = configFile.getString("kugsha.profiles.logfile.dateFormat")

    val lines = Source.fromFile(logPath).getLines.toList

    lines.flatMap { line: String =>
      val lineSplited: List[String] = line.split(delimiter).toList
      // userId, timestamp, url
      if (!ignoreList.exists(lineSplited.get(urlPosition).contains(_))) {
        val allUrl: Uri = lineSplited.get(urlPosition)
        val canon: Uri = allUrl.removeAllParams()
        Some(
          LogEntry(
            lineSplited.get(userIdPosition),
            DateTimeFormat.forPattern(dateFormat).parseDateTime(lineSplited.get(timestampPosition)),
            canon.toString.stripSuffix("/")
          )
        )
      } else None
    }
  }

  def ParseJsonLog() = {
    val logPath = configFile.getString("kugsha.profiles.logfile.path")
    val dateFormat = configFile.getString("kugsha.profiles.logfile.dateFormat")
    val domain = configFile.getString("kugsha.crawler.domain").replace("www.", "")
    val res = mutable.ListBuffer[LogEntry]()

    Source.fromFile(logPath, "UTF-8").getLines.foreach { line =>

      val json = Json.parse(line)

      (json \ "meta" \ "type").asOpt[String] match {
        case Some(_) =>
          val url = Uri.parse((json \ "uri" \ "query" \ "location").asOpt[String].getOrElse("")).removeAllParams
          if (url.contains(domain)) {
            val uid = (json \ "meta" \ "uid").asOpt[String]
            val timestamp = (json \ "meta" \ "timestamp").asOpt[String]
            val cat = (json \ "uri" \ "query" \ "category").asOpt[String]
            val typ = (json \ "meta" \ "type").asOpt[String]
            if (uid.isDefined && timestamp.isDefined) {
              pages += (url.toString -> LogPageEntry(url.toString, typ.getOrElse("NotDefined"), cat.getOrElse("NotDefined")))
              res += LogEntry(uid.get, DateTimeFormat.forPattern(dateFormat).parseDateTime(timestamp.get), url.toString)
            }
          }
        case None => None
      }
    }
    res.toList
  }

  def getUrlInfoDb(url: String): (Option[List[String]], Option[String]) = {

    val coll = db.getCollection(collectionName)

    val query = Document("url" -> url)

    coll.find(query).results().headOption.map { page =>
      val category = page.get[BsonArray]("category") match {
        case Some(cat) => Some(cat.getValues.map(_.asString.getValue).toList)
        case _ => None
      }
      val kind = page.get[BsonString]("type") match {
        case Some(typ) => Some(typ.getValue)
        case _ => None
      }
      (category, kind)
    }.getOrElse((None, None))
  }

  def getUrlInfoLogs(url: String): (Option[List[String]], Option[String]) = {
    pages.get(url) match {
      case Some(r) => (Some(List(r.category)), Some(r.kind))
      case _ => (None, None)
    }
  }

  def sessions(records: List[LogEntry]) = {
    records.foreach { rec =>
      {
        users.get(rec.id) match {
          case Some(p) =>
            if ((rec.timestamp.getMillis - p.firstTime.getMillis) > (15 * 60 * 1000)) {
              val newSession = (ListBuffer(rec.url), 0l)
              p.visitedPages += newSession
              p.firstTime = rec.timestamp
            } else {
              p.visitedPages.last._1 += rec.url
              val temp = (p.visitedPages.last._1, rec.timestamp.getMillis - p.firstTime.getMillis)
              p.visitedPages.trimEnd(1)
              p.visitedPages += temp
            }
          case None => users += (rec.id -> Profile(rec.id, mutable.HashMap(), mutable.HashMap(), ListBuffer((ListBuffer(rec.url), 0l)), rec.timestamp, 0l))
        }
      }
    }

    users.foreach { (u: (String, Profile)) =>
      {
        val listCat = ListBuffer[String]()
        val listType = ListBuffer[String]()

        val allPages: ListBuffer[String] = u._2.visitedPages.flatMap(p => p._1)

        val averageSessionTime = u._2.visitedPages.foldLeft(0l)((r, p) => r + (p._2 / u._2.visitedPages.size))

        allPages.foreach { url =>
          {

            if (isJSON) {
              val info = getUrlInfoLogs(url)
              if (info._1.isDefined)
                listCat ++= info._1.get
              if (info._2.isDefined)
                listType += info._2.get
            } else {
              val info = getUrlInfoDb(url)
              if (info._1.isDefined)
                listCat ++= info._1.get
              if (info._2.isDefined)
                listType += info._2.get
            }

          }
        }
        val weightsProducts = mutable.HashMap[String, Double]()
        listCat.map(cat => {
          weightsProducts.get(cat) match {
            case Some(c) => weightsProducts += (cat -> (c + (1.0 / listCat.size)))
            case None => weightsProducts += (cat -> (1.0 / listCat.size))
          }
        })

        val weightsTypes = mutable.HashMap[String, Double]()
        listType.map(typ => {
          weightsTypes.get(typ) match {
            case Some(t) => weightsTypes += (typ -> (t + (1.0 / listType.size)))
            case None => weightsTypes += (typ -> (1.0 / listType.size))
          }
        })

        users += (u._1 -> Profile(u._1, weightsProducts, weightsTypes, u._2.visitedPages, u._2.firstTime, averageSessionTime))
      }
    }
  }

  def saveProfiles(users: mutable.HashMap[String, Profile]) = {
    users.foreach { u =>
      {
        val collection: MongoCollection[Document] = db.getCollection(configFile.getString("kugsha.crawler.domain") + "-profiles")
        val document: Document = Document(
          "_id" -> u._1,
          "flowSequence" -> u._2.visitedPages.map(p => Document("flow" -> p._1.toList, "time" -> p._2 / 1000)).toList,
          "preferences" -> u._2.preferencesProbabilities.toList,
          "pageTypes" -> u._2.pageTypeProbabilities.toList,
          "average" -> u._2.averageTime / 1000
        )
        collection.insertOne(document).headResult()
      }
    }

  }
}
