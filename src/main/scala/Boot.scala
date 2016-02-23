import java.util.concurrent.TimeUnit

import classification.Categorization
import crawler.Crawler
import com.typesafe.config.ConfigFactory
import org.mongodb.scala._
import collection.JavaConversions._
import database.Helpers._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Boot {

  val configFile = ConfigFactory.load()
  val host = configFile.getString("kugsha.database.host")
  val port = configFile.getInt("kugsha.database.port")
  val dbname = configFile.getString("kugsha.database.dbname")
  val collectionName = configFile.getString("kugsha.database.collectionName")

  val connString = "mongodb://" + host + ":" + port

  val protocol = configFile.getString("kugsha.crawler.protocol")
  val domain = configFile.getString("kugsha.crawler.domain")
  val startPage = configFile.getString("kugsha.crawler.startPage")
  val linkRegex = configFile.getString("kugsha.crawler.linkRegex")
  val ignoreList = configFile.getStringList("kugsha.crawler.ignoreList").toList
  val ignoreUrlWithList = configFile.getStringList("kugsha.crawler.ignoreUrlWithList").toList
  val encoding = configFile.getString("kugsha.crawler.encoding")

  val classifierSelector = configFile.getStringList("kugsha.classification.categories").toList

  def main(args: Array[String]) {
    val client: MongoClient = MongoClient(connString)
    val db: MongoDatabase = client.getDatabase(dbname)
    /* val crawler = new Crawler(protocol+domain, domain, startPage, linkRegex, ignoreList, ignoreUrlWithList, db, collectionName, encoding)
    crawler.start*/

    val categorization = new Categorization(db, collectionName, classifierSelector)
    categorization.classifyTask
    println("finished")

    client.close()
  }

}