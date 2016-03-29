import java.util.concurrent.TimeUnit

import classification.Categorization
import com.typesafe.config.ConfigFactory
import crawler.Crawler
import database.Helpers._
import logfile.Parse
import org.bson.BsonString
import org.graphstream.ui.view.{ View, Viewer }
import org.mongodb.scala._

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

object Boot {

  val configFile = ConfigFactory.load("kk")
  val collectionName = configFile.getString("kugsha.database.collectionName")
  val connString = configFile.getString("kugsha.database.connString")
  val dbname = configFile.getString("kugsha.database.dbname")

  val protocol = configFile.getString("kugsha.crawler.protocol")
  val domain = configFile.getString("kugsha.crawler.domain")
  val startPage = configFile.getString("kugsha.crawler.startPage")
  val ignoreList = configFile.getStringList("kugsha.crawler.ignoreList").toList
  val ignoreUrlWithList = configFile.getStringList("kugsha.crawler.ignoreUrlWithList").toList
  val encoding = configFile.getString("kugsha.crawler.encoding")
  val ignoreParams = configFile.getStringList("kugsha.crawler.ignoreParams").toList

  import org.graphstream.graph.implementations._

  def draw(graph: MultiGraph, db: MongoDatabase): Future[MultiGraph] = Future {
    db.getCollection(collectionName).find().results().foreach { page =>
      {
        page.get("outbound").get.asArray().foreach(x => {
          val ori = page.getOrElse("url", "").asInstanceOf[BsonString].getValue
          val dest = x.asInstanceOf[BsonString].getValue
          println(ori + " -> " + dest)
          graph.addEdge(ori + dest, ori, dest, true).asInstanceOf[AbstractEdge]
        })
      }
    }
    for (n <- graph) {
      n.setAttribute("label", n.getId)
    }
    graph
  }

  def main(args: Array[String]) {
    val client: MongoClient = MongoClient(connString)
    val db: MongoDatabase = client.getDatabase(dbname)
    /*val crawler = new Crawler(protocol + domain, domain, startPage, ignoreList, ignoreUrlWithList, db, collectionName, encoding, ignoreParams)
    crawler.start
*/
    /* val categorization = new Categorization(db, collectionName, configFile)
    categorization.classifyTask*/

    /*val graph = new MultiGraph("")
    graph.addAttribute("ui.label", "text-mode:normal")
    graph.setStrict(false)
    graph.setAutoCreate(true)
    graph.addAttribute("ui.stylesheet", "node {fill-color: red; size-mode: dyn-size;} edge {fill-color:grey;}")

    Await.result(draw(graph, db), Duration(20, TimeUnit.SECONDS)).display()
*/
    //JSON vs Normal
    val parse = new Parse(configFile, db, collectionName, true)

    //TODO: change automatic by true/false
    parse.sessions(parse.ParseJsonLog())

    parse.saveProfiles(parse.users)

    println("Finished.")

    client.close
  }

}