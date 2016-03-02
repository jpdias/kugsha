package classification

import java.util.concurrent.TimeUnit

import com.netaporter.uri.Uri
import com.netaporter.uri.Uri._
import database.Helpers._
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

import scala.collection.JavaConversions._

class Categorization(db: MongoDatabase, collectionName: String, selectorList: List[String]) {
  def classifyTask = {
    db.getCollection(collectionName).find().results().foreach { page =>
      {
        Await.result(findAndSetCategory(page), Duration(10, TimeUnit.SECONDS))
      }
    }
  }

  def findAndSetCategory(page: Document) = Future {
    val doc = Jsoup.parse(page.get("content").toString)
    val url: Uri = parse(page.get("url").toString)

    val categories = doc.select("td>a.branco3:not(:first-child)")

    val productPage = doc.select("#atab1.brancoxx")
    val productList = doc.select("#produtos_normais")
    if (!categories.isEmpty) {
      var cats: ListBuffer[String] = ListBuffer()
      for (el: Element <- categories) {
        cats += el.text
      }
      val rescats = Document("$set" -> Document("category" -> cats.map(x => x.toString).toList))
      //db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), unset("category")).headResult()
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), rescats).headResult()
    }
    if (!productPage.isEmpty && url.toString.contains("/2/")) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "product")).headResult()
    } else if (!productList.isEmpty && url.toString.contains("/1/")) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "list")).headResult()
    } else if (url.toString.contains("nm_carrinho")) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "cart")).headResult()
    } else {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "generic")).headResult()
    }
  }
}
