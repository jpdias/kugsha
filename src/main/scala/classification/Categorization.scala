package classification

import java.util.concurrent.TimeUnit

import database.Helpers._
import org.jsoup.Jsoup
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

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
    val cat: String = doc.select(selectorList.head).text()
    if (cat.isEmpty) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("category", "Information")).headResult()
    } else {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("category", cat)).headResult()
    }
  }
}
