package classification

import com.netaporter.uri.Uri
import com.netaporter.uri.Uri._
import com.typesafe.config.Config
import database.Helpers._
import org.jsoup.Jsoup
import org.jsoup.nodes.Element
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonString
import org.mongodb.scala.model.Filters._

import scala.collection.JavaConversions._
import scala.collection._

class Categorization(
    db: MongoDatabase, collectionName: String, configFile: Config
) {

  private val categoryTree =
    mutable.HashMap[List[String], mutable.Set[String]]()

  def classifyTask() = {
    var count = 0
    val total = db.getCollection(collectionName).count().headResult()
    while (total > count) {
      val queryRes = db
        .getCollection(collectionName)
        .find(Document("_id" -> count))
        .results()
        .headOption
      queryRes match {
        case Some(p) => findAndSetCategory(p)
        case _ => println("Not Found")
      }
      count += 1
      if (count % 3000 == 0)
        println(count)
    }

    val tree = categoryTree.keySet.map(k =>
      Document(
        "parent" -> k, "children" -> categoryTree.get(k).get.toList
      ))
    val categoryTreeCollection =
      configFile.getString("kugsha.classification.categories.collectionName")
    tree.map(
      db.getCollection(categoryTreeCollection).insertOne(_).headResult()
    )
  }

  def findAndSetCategory(page: Document) = {

    val doc = Jsoup.parse(page.get("content").get.toString)
    val url: Uri = parse(page.get[BsonString]("url").get.getValue)

    val categories = doc.select(configFile.getString(
      "kugsha.classification.selectors.categoriesArray"
    ))
    val productPage = doc.select(
      configFile.getString("kugsha.classification.selectors.productPage")
    )
    val productList = doc.select(configFile.getString(
      "kugsha.classification.selectors.productListPage"
    ))
    val cartPage = try {
      doc.select(
        configFile.getString("kugsha.classification.selectors.cartPage")
      )
    } catch { case e: Throwable => "" }

    val price =
      doc.select(configFile.getString("kugsha.classification.selectors.price"))
    val prodName = doc.select(
      configFile.getString("kugsha.classification.selectors.productName")
    )
    val isDynamic = doc.select(
      configFile.getString("kugsha.classification.selectors.dynamicPart")
    )

    var updateAll = Document()

    if (!categories.isEmpty) {
      val catList = categories.map(cat => cat.text).toList

      val categoryDepth =
        configFile.getInt("kugsha.classification.categories.categoryDepth")
      //create category tree with predefined category deepness
      categoryTree.get(catList.take(categoryDepth)) match {
        case Some(k) => k ++= catList.drop(categoryDepth)
        case None =>
          categoryTree += catList.take(categoryDepth) -> catList
            .drop(categoryDepth)
            .to[mutable.Set]
      }
      updateAll ++= Document("category" -> catList)
    }

    //val urlRegexExists = configFile.hasPath("kugsha.classification.urlRegex")
    /*if (urlRegexExists) {
    if (!productPage.isEmpty && url.toString.matches(configFile.getString("kugsha.classification.urlRegex.productPage"))) {
       updateAll ++= Document("type" -> "product")
       updateAll ++= Document("price" -> price.text)
       updateAll ++= Document("productName" -> prodName.text)
     } else if (!productList.isEmpty && url.toString.contains(configFile.getString("kugsha.classification.urlRegex.productListPage"))) {
       updateAll ++= Document("type" -> "list")
     } else if (!price.isEmpty) {
       updateAll ++= Document("cart" -> true)
     } else {
       updateAll ++= Document("type" -> "generic")
     }
   } else {*/
    if (productPage.nonEmpty || url.toString.matches(configFile.getString(
      "kugsha.classification.urlRegex.productPage"
    ))) {
      updateAll ++= Document("type" -> "product")
      updateAll ++= Document("price" -> price.text)
      updateAll ++= Document("productName" -> prodName.text)
      if (!price.isEmpty)
        updateAll ++= Document("cart" -> true)
    } else if (productList.nonEmpty) {
      updateAll ++= Document("type" -> "list")
    } else if (cartPage.toString.nonEmpty || url.toString.contains(configFile
      .getString("kugsha.classification.urlRegex.cartPage"))) {
      updateAll ++= Document("type" -> "cart")
    } else {
      updateAll ++= Document("type" -> "generic")
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

    db.getCollection(collectionName)
      .updateOne(
        equal("_id", page.get("_id").get), Document("$set" -> updateAll)
      )
      .headResult()
  }
}
