import com.typesafe.config.ConfigFactory
import database.Helpers._
import logfile.Clustering
import org.bson.BsonString
import org.mongodb.scala._

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Boot {

  val configFile = ConfigFactory.load("kk")
  val collectionName = configFile.getString("kugsha.database.collectionName")
  val profilesCollectionName = configFile.getString("kugsha.database.profilesCollectionName")
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

    println("1 - Crawler")
    //Crawler Step
    //val crawler = new Crawler(protocol + domain, domain, startPage, ignoreList, ignoreUrlWithList, db, collectionName, encoding, ignoreParams)
    //crawler.start

    println("2 - Page info extract")
    //Page information extraction + category tree
    //val categorization = new Categorization(db, collectionName, configFile)
    //categorization.classifyTask

    /*val graph = new MultiGraph("")
    graph.addAttribute("ui.label", "text-mode:normal")
    graph.setStrict(false)
    graph.setAutoCreate(true)
    graph.addAttribute("ui.stylesheet", "node {fill-color: red; size-mode: dyn-size;} edge {fill-color:grey;}")

    Await.result(draw(graph, db), Duration(20, TimeUnit.SECONDS)).display()
*/
    println("3 - Logger parse and session split")
    //Log Parse and Session splitting
    //val parse = new Parse(configFile, db, collectionName, configFile.getBoolean("kugsha.profiles.isJson"))
    //parse.sessions(parse.ParseLog())
    //parse.saveProfiles(parse.users)

    println("4 - Clustering")
    //Clutering Profiles
    val newClustering = new Clustering(configFile, db, profilesCollectionName)
    newClustering.loadData

    println("Finished.")

    client.close
  }

}