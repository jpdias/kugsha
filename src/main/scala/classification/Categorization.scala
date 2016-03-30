package classification

import java.util.concurrent.TimeUnit

import com.netaporter.uri.Uri
import com.netaporter.uri.Uri._
import com.typesafe.config.Config
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

class Categorization(db: MongoDatabase, collectionName: String, configFile: Config) {
  def classifyTask(): Unit = {
    db.getCollection(collectionName).find().results().foreach { page =>
      {
        Await.result(findAndSetCategory(page), Duration(10, TimeUnit.SECONDS))
      }
    }
  }

  def findAndSetCategory(page: Document) = Future {
    val doc = Jsoup.parse(page.get("content").toString)
    val url: Uri = parse(page.get("url").toString)

    val categories = doc.select(configFile.getString("kugsha.classification.selectors.categoriesArray"))
    val productPage = doc.select(configFile.getString("kugsha.classification.selectors.productPage"))
    val productList = doc.select(configFile.getString("kugsha.classification.selectors.productListPage"))

    val price = doc.select(configFile.getString("kugsha.classification.selectors.price"))
    val prodName = doc.select(configFile.getString("kugsha.classification.selectors.productName"))
    val isDynamic = doc.select(configFile.getString("kugsha.classification.selectors.dynamicPart"))

    if (!categories.isEmpty) {
      var cats: ListBuffer[String] = ListBuffer()
      for (el: Element <- categories) {
        cats += el.text
      }
      val rescats = Document("$set" -> Document("category" -> cats.map(x => x.toString).toList))
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), rescats).headResult()
    }
    if (!productPage.isEmpty && url.toString.matches(configFile.getString("kugsha.classification.urlRegex.productPage"))) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "product")).headResult()
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("price", price.text)).headResult()
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("productName", prodName.text)).headResult()
    } else if (!productList.isEmpty && url.toString.contains(configFile.getString("kugsha.classification.urlRegex.productListPage"))) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "list")).headResult()
    } else if (url.toString.contains(configFile.getString("kugsha.classification.urlRegex.cartPage"))) {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "cart")).headResult()
    } else {
      db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("type", "generic")).headResult()
    }

    val dynamicExists = !isDynamic.isEmpty

    db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), set("isDynamic", dynamicExists)).headResult()

  }
}
