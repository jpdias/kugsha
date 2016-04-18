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
    var count = 0
    val total = db.getCollection(collectionName).count().headResult()
    while (total > count) {
      val queryRes = db.getCollection(collectionName).find(Document("_id" -> count)).results().headOption
      queryRes match {
        case Some(p) => findAndSetCategory(p)
        case _ => println("Not Found")
      }
      count += 1
      if (count % 3000 == 0)
        println(count)
    }

    // Lets run a query for all Martins and print out the json representation of each document
    /*val query = db.getCollection(collectionName).find().subscribe(
      (page: Document) => findAndSetCategory(page),                         // onNext
      (error: Throwable) => println(s"Query failed: ${error.getMessage}"), // onError
      () => println("Done")                                               // onComplete
    )*/
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

    var updateAll = Document()

    if (!categories.isEmpty) {
      var cats: ListBuffer[String] = ListBuffer()
      for (el: Element <- categories) {
        cats += el.text
      }
      updateAll ++= Document("category" -> cats.map(x => x.toString).toList)
    }

    val urlRegexExists = configFile.hasPath("kugsha.classification.urlRegex")
    if (urlRegexExists) {
      if (!productPage.isEmpty && url.toString.matches(configFile.getString("kugsha.classification.urlRegex.productPage"))) {
        updateAll ++= Document("type" -> "product")
        updateAll ++= Document("price" -> price.text)
        updateAll ++= Document("productName" -> prodName.text)
      } else if (!productList.isEmpty && url.toString.contains(configFile.getString("kugsha.classification.urlRegex.productListPage"))) {
        updateAll ++= Document("type" -> "list")
      } else if (!price.isEmpty) {
        updateAll ++= Document("cart" -> true)
      } else {
        updateAll ++: Document("type" -> "generic")
      }
    } else {
      if (!productPage.isEmpty) {
        updateAll ++= Document("type" -> "product")
        updateAll ++= Document("price" -> price.text)
        updateAll ++= Document("productName" -> prodName.text)
        if (!price.isEmpty)
          updateAll ++= Document("cart" -> true)
      } else if (!productList.isEmpty) {
        updateAll ++= Document("type" -> "list")
      } else {
        updateAll ++: Document("type" -> "generic")
      }
    }
    val dynamicExists = !isDynamic.isEmpty

    updateAll ++= Document("isDynamic" -> dynamicExists)

    if (dynamicExists) {
      var dynamicCount = 0
      for (el: Element <- isDynamic) {
        dynamicCount += el.children().length
      }
      updateAll ++= Document("dynamicCount" -> dynamicCount)
    }

    db.getCollection(collectionName).updateOne(equal("_id", page.get("_id").get), Document("$set" -> updateAll)).headResult()
  }
}
