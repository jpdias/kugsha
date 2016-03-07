package logfile

import com.netaporter.uri.Uri
import com.typesafe.config.Config
import com.netaporter.uri.dsl._
import org.bson.BsonArray
import org.mongodb.scala._
import org.mongodb.scala.bson.collection.mutable.Document

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io._
import scala.collection.JavaConversions._
import database.Helpers._

case class Profile(id: String, preferencesProbabilities: mutable.HashMap[String, Double], visitedPages: mutable.ListBuffer[String])

class Parse(logPath: String, configFile: Config, db: MongoDatabase, collectionName: String) {

  val lines = Source.fromFile(logPath).getLines.toList
  val users = mutable.HashMap[String, Profile]()

  def ParseLog() = {
    val delimiter = configFile.getString("kugsha.profiles.logfile.delimiter")
    val userIdPosition = configFile.getInt("kugsha.profiles.logfile.userIdPosition")
    val timestampPosition = configFile.getInt("kugsha.profiles.logfile.timestampPosition")
    val urlPosition = configFile.getInt("kugsha.profiles.logfile.urlPosition")
    val ignoreList = configFile.getStringList("kugsha.profiles.logfile.ignoreList").toList

    val protocol = configFile.getString("kugsha.crawler.protocol")
    val base = configFile.getString("kugsha.crawler.domain")

    val res = lines.map { line: String =>
      val lineSplited: List[String] = line.split(delimiter).toList
      // userId, timestamp, url
      if (!ignoreList.exists(lineSplited.get(urlPosition).contains(_))) {
        val allUrl: Uri = protocol + "www." + base + lineSplited.get(urlPosition)
        val canon: Uri = allUrl.removeAllParams()
        List(lineSplited.get(userIdPosition), lineSplited.get(timestampPosition), canon.toString.stripSuffix("/"))
      } else
        List()
    }.filter(_.nonEmpty)
    res
  }

  def sessions(records: List[List[String]]) = {
    records.map { rec =>
      {
        val profile = users.get(rec.head) match {
          case Some(p) => p
          case None => Profile(rec.head, mutable.HashMap(), ListBuffer())
        }
        profile.visitedPages += rec.get(2)
        users += (rec.head -> profile)
      }
    }
    val coll = db.getCollection(collectionName)
    users.foreach { (u: (String, Profile)) =>
      {
        val listCat = ListBuffer[String]()
        u._2.visitedPages.foreach { url =>
          {
            val query = Document("url" -> url)

            coll.find(query).results().foreach { page =>
              page.get[BsonArray]("category") match {
                case Some(cat) => listCat ++= cat.getValues.map(_.asString.getValue)
                case _ => println("Not Found")
              }
            }
          }
        }
        val weights = mutable.HashMap[String, Double]()
        listCat.map(cat => {
          weights.get(cat) match {
            case Some(c) => weights += (cat -> (c + (1.0 / listCat.size)))
            case None => weights += (cat -> (1.0 / listCat.size))
          }
        })

        users += (u._1 -> Profile(u._1, weights, u._2.visitedPages))
      }
    }
  }

  def saveProfiles = {
    users.foreach { u =>
      {
        val collection: MongoCollection[Document] = db.getCollection("profiles")
        val document: Document = Document("_id" -> u._1, "flowSequence" -> u._2.visitedPages, "preferences" -> u._2.preferencesProbabilities.toList)
        collection.insertOne(document).headResult()
      }
    }

  }
}
