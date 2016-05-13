package logfile

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

import scala.collection.breakOut
import scala.collection.generic.CanBuildFrom

case class Profile(
  id:                       String,
  preferencesProbabilities: mutable.HashMap[String, Double],
  pageTypeProbabilities:    mutable.HashMap[String, Double],
  visitedPages:             mutable.ListBuffer[(mutable.ListBuffer[String], Long)],
  var firstTime:            DateTime,
  averageTime:              Option[Long],
  totalPageViews:           Option[Int],
  sessionInfo:              List[SessionDetail],
  sessionResume:            Option[SessionDetail]
)

case class innerInfo(cat: Int, value: Double)

case class SessionDetail(
  sessionLength:   innerInfo,
  sessionDuration: innerInfo,
  meanTimePerPage: innerInfo
)

case class LogEntry(id: String, timestamp: DateTime, url: String)

case class LogPageEntry(url: String, kind: String, category: String)

class Parse(configFile: Config, db: MongoDatabase, collectionName: String, isJSON: Boolean) {

  def mode[T, CC[X] <: Seq[X]](coll: CC[T])(implicit o: T => Ordered[T], cbf: CanBuildFrom[Nothing, T, CC[T]]): CC[T] = {
    val grouped = coll.groupBy(x => x).mapValues(_.size).toSeq
    val max = grouped.map(_._2).max
    grouped.filter(_._2 == max).map(_._1)(breakOut)
  }

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  val users = mutable.HashMap[String, Profile]()
  val pages = mutable.HashMap[String, LogPageEntry]()

  def ParseLog() = {
    if (isJSON) {
      ParseJsonLog()
    } else {
      ParseServerLog()
    }
  }

  def ParseServerLog() = {
    val delimiter = configFile.getString("kugsha.profiles.logfile.delimiter")
    val userIdPosition = configFile.getInt("kugsha.profiles.logfile.userIdPosition")
    val timestampPosition = configFile.getInt("kugsha.profiles.logfile.timestampPosition")
    val urlPosition = configFile.getInt("kugsha.profiles.logfile.urlPosition")
    val ignoreList = configFile.getStringList("kugsha.profiles.logfile.ignoreList").toList
    val logPath = configFile.getString("kugsha.profiles.path")
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
    val logPath = configFile.getString("kugsha.profiles.path")
    val dateFormat = configFile.getString("kugsha.profiles.logfile.dateFormat")
    val domain = configFile.getString("kugsha.crawler.domain").replace("www.", "")

    val product = configFile.getStringList("kugsha.profiles.jsonfile.pageMap.product")
    val list = configFile.getStringList("kugsha.profiles.jsonfile.pageMap.list")
    val cart = configFile.getStringList("kugsha.profiles.jsonfile.pageMap.cart")
    val generic = configFile.getStringList("kugsha.profiles.jsonfile.pageMap.generic")

    val res = mutable.ListBuffer[LogEntry]()

    Source.fromFile(logPath, "UTF-8").getLines.foreach { line =>

      val json = Json.parse(line)

      val local = (json \ "uri" \ "query" \ "location").asOpt[String]
      val eventType = (json \ "uri" \ "query" \ "type").asOpt[String]

      if (local.isDefined) {

        val url = Uri.parse(local.get).removeAllParams

        if (url.contains(domain) && eventType.isDefined && eventType.get.equals("pageView")) {

          val uid = (json \ "meta" \ "uid").asOpt[String]
          val timestamp = (json \ "meta" \ "timestamp").asOpt[String]
          val cat = (json \ "uri" \ "query" \ "category").asOpt[String]

          val pageType = (json \ "uri" \ "query" \ "pageType").asOpt[String].getOrElse("notDefined")

          val kind = if (product.contains(pageType))
            "product"
          else if (list.contains(pageType))
            "list"
          else if (cart.contains(pageType))
            "cart"
          else if (generic.contains(pageType))
            "generic"
          else
            "notDefined"

          val cate = if (cat.isDefined && cat.get.isEmpty) { Some("notDefined") } else cat

          if (uid.isDefined && timestamp.isDefined) {
            pages += (url.toString -> LogPageEntry(url.toString, kind, cate.getOrElse("notDefined")))
            res += LogEntry(uid.get, DateTimeFormat.forPattern(dateFormat).parseDateTime(timestamp.get), url.toString)
          }
        }
      } else if (eventType.isDefined && eventType.get.equals("productClickPaid")) {
        val uid = (json \ "meta" \ "uid").asOpt[String]
        val timestamp = (json \ "meta" \ "timestamp").asOpt[String]

        if (uid.isDefined && timestamp.isDefined) {
          pages += ("/cart" -> LogPageEntry("/cart", "cart", "cart"))
          res += LogEntry(uid.get, DateTimeFormat.forPattern(dateFormat).parseDateTime(timestamp.get), "/cart")
        }
      }
    }
    res.toList
  }

  def getUrlInfoDb(url: String): (Option[List[String]], Option[String]) = {

    val coll = db.getCollection(collectionName)

    val query = Document("url" -> url)

    coll.find(query).results().headOption.map { page =>
      val category = page.get[BsonArray]("category") match {
        case Some(cat) => Some(cat.getValues.map(_.asString.getValue.replace(".", "").replace("$", "")).toList)
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
      case Some(r) => (Some(List(r.category.replace(".", "").replace("$", ""))), Some(r.kind))
      case _ => (None, None)
    }
  }

  def sessions(records: List[LogEntry]) = {
    records.foreach { rec =>
      {
        users.get(rec.id) match {
          case Some(p) =>
            val sessionThresholdTime = configFile.getInt("kugsha.profiles.sessionTimeThreshold")
            if ((rec.timestamp.getMillis - p.firstTime.getMillis) > (sessionThresholdTime * 60 * 1000)) {
              val newSession = (ListBuffer(rec.url), 0l)
              p.visitedPages += newSession
              p.firstTime = rec.timestamp
            } else {
              p.visitedPages.last._1 += rec.url
              val temp = (p.visitedPages.last._1, rec.timestamp.getMillis - p.firstTime.getMillis)
              p.visitedPages.trimEnd(1)
              p.visitedPages += temp
            }
          case None => users += (
            rec.id -> Profile(rec.id, mutable.HashMap(), mutable.HashMap(), ListBuffer((ListBuffer(rec.url), 0l)), rec.timestamp, None, None, List(), None)
          )
        }
      }
    }

    users.foreach { (u: (String, Profile)) =>
      {
        val listCat = ListBuffer[String]()
        val listType = ListBuffer[String]()

        val allPages: ListBuffer[String] = u._2.visitedPages.flatMap(p => p._1)

        val averageSessionTime = u._2.visitedPages.foldLeft(0l)((r, p) => r + (p._2 / u._2.visitedPages.size))

        val averageTimePerPage = averageSessionTime / u._2.visitedPages.map(x => x._1.length).sum

        allPages.foreach { url =>
          {
            val info = if (isJSON) {
              getUrlInfoLogs(url)
            } else {
              getUrlInfoDb(url)
            }

            if (info._1.isDefined)
              listCat ++= info._1.get
            if (info._2.isDefined)
              listType += info._2.get
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

        val sessionInformation: List[SessionDetail] = u._2.visitedPages.map { en =>
          {

            val sessionLengthDefault = configFile.getConfig("kugsha.profiles.session.sessionLength")
            val sessionDurationDefault = configFile.getConfig("kugsha.profiles.session.sessionDuration")
            val meanTimePerPageDefault = configFile.getConfig("kugsha.profiles.session.meanTimePerPage")

            val sessionLength = if (en._1.length <= sessionLengthDefault.getDouble("short"))
              innerInfo(0, en._1.size)
            else if (en._1.length > sessionLengthDefault.getDouble("long"))
              innerInfo(2, en._1.size)
            else
              innerInfo(1, en._1.size)

            val sessionDuration = if (en._2 <= sessionDurationDefault.getDouble("short"))
              innerInfo(0, en._2)
            else if (en._2 > sessionDurationDefault.getDouble("long"))
              innerInfo(2, en._2)
            else
              innerInfo(1, en._2)

            val meanTimePerPage = if (en._2 / en._1.size <= meanTimePerPageDefault.getDouble("short"))
              innerInfo(0, en._2 / en._1.size)
            else if (en._2 > meanTimePerPageDefault.getDouble("long"))
              innerInfo(2, en._2 / en._1.size)
            else
              innerInfo(1, en._2 / en._1.size)

            SessionDetail(sessionLength, sessionDuration, meanTimePerPage)
          }
        }.toList

        val sessionResume = if (sessionInformation.nonEmpty)
          Some(SessionDetail(
            innerInfo(average(sessionInformation.map(x => x.sessionLength.cat)).toInt, average(sessionInformation.map(x => x.sessionLength.value)).toD),
            innerInfo(average(sessionInformation.map(x => x.sessionDuration.cat)).toInt, average(sessionInformation.map(x => x.sessionDuration.value))),
            innerInfo(average(sessionInformation.map(x => x.meanTimePerPage.cat)).toInt, average(sessionInformation.map(x => x.meanTimePerPage.value)))
          ))
        else
          None

        users += (
          u._1 -> Profile(u._1, weightsProducts, weightsTypes, u._2.visitedPages, u._2.firstTime, Some(averageSessionTime), Some(allPages.size), sessionInformation, sessionResume)
        )
      }
    }
  }

  def saveProfiles(users: mutable.HashMap[String, Profile]) = {
    users.foreach { u =>
      {
        val collection: MongoCollection[Document] = db.getCollection(configFile.getString("kugsha.database.profilesCollectionName"))
        val document = Document(
          "_id" -> u._1,
          "flowSequence" -> u._2.visitedPages.map(p => Document(
            "flow" -> p._1.toList,
            "time" -> Math.abs(p._2 / 1000)
          )).toList,
          "preferences" -> u._2.preferencesProbabilities.toList,
          "pageTypes" -> u._2.pageTypeProbabilities.toList,
          "averageSessionTime" -> Math.abs(u._2.averageTime.getOrElse(0l) / 1000.0),
          "totalPageViews" -> u._2.totalPageViews.getOrElse(0),
          "sessionInformation" -> u._2.sessionInfo.map(
            s => List(
              Document("meanTimePerPage" -> Document("level" -> s.meanTimePerPage.cat, "value" -> s.meanTimePerPage.value / 1000.0)),
              Document("sessionDuration" -> Document("level" -> s.sessionDuration.cat, "value" -> s.sessionDuration.value / 1000.0)),
              Document("sessionLength" -> Document("level" -> s.sessionLength.cat, "value" -> s.sessionLength.value / 1000.0))
            )
          )
        )
        u._2.sessionResume match {
          case Some(s) => document.update("sessionResume", List(
            Document("meanTimePerPage" -> Document("level" -> s.meanTimePerPage.cat, "value" -> s.meanTimePerPage.value / 1000.0)),
            Document("sessionDuration" -> Document("level" -> s.sessionDuration.cat, "value" -> s.sessionDuration.value / 1000.0)),
            Document("sessionLength" -> Document("level" -> s.sessionLength.cat, "value" -> s.sessionLength.value / 1000.0))
          ))
          case None =>
        }
        collection.insertOne(document).headResult()
      }
    }
  }
}
