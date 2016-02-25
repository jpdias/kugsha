package crawler

import org.jsoup.{ nodes, Jsoup }
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import org.mongodb.scala._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.collection.mutable
import collection.JavaConversions._

class Crawler(baseUrl: String, domain: String, startPage: String = "/", ignoreList: List[String], ignoreUrlWithList: List[String], db: MongoDatabase, colectionName: String, encoding: String) {

  var visited = List[String]()
  val frontier = new mutable.Queue[String]

  def getLinks(html: String): List[String] = {
    val doc: nodes.Document = Jsoup.parse(html)
    val links: Elements = doc.select("a[href]")
    val refs = mutable.Set[String]()
    for (link: Element <- links) {
      val ext = link.attr("abs:href").stripSuffix("/").replaceAll("""#(.+)""", "").stripSuffix("/")
      refs += ext
    }
    refs.toList
  }

  def getHttp(url: String) = {
    try {
      val in = Source.fromURL(url, encoding)
      val response = in.getLines.mkString
      in.close()
      response
    } catch {
      case e: Exception => throw new Exception("InvalidRequest: " + e.getMessage)
    }
  }

  def writePageToDb(pageCount: Int, url: String, pageContent: String, outboundLinks: List[String]) = Future {
    val collection: MongoCollection[Document] = db.getCollection(colectionName)
    val document: Document = Document("_id" -> pageCount, "url" -> url, "content" -> pageContent, "outbound" -> outboundLinks)
    val insertObservable: Observable[Completed] = collection.insertOne(document)

    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")
      override def onError(e: Throwable): Unit = println(s"onError: $e")
      override def onComplete(): Unit = println("onComplete")
    })

  }

  def getFilteredPages(pageContent: String): Set[String] = {
    val outboundLinks = getLinks(pageContent)
    val outboundLinksFilterExtensions = outboundLinks.filter(x => !ignoreList.exists(x.endsWith))
    val outboundLinksFilterUrlParts = outboundLinksFilterExtensions.filter(x => !ignoreUrlWithList.contains(x))
    val outboundLinksFilterDomain = outboundLinksFilterUrlParts.filter(x => x.contains(domain))
    outboundLinksFilterDomain.toSet
  }

  def start = {
    var pageCount = 0

    val pageContent = getHttp(baseUrl + startPage)
    val outboundLinks = getFilteredPages(pageContent)
    frontier ++= outboundLinks

    writePageToDb(pageCount, baseUrl + startPage, pageContent, outboundLinks.toList)

    while (frontier.nonEmpty) {
      println(frontier.size)
      val link: String = frontier.dequeue()
      visited ++= List[String](link)
      try {
        println(link)
        val pageContent = getHttp(link)
        val outboundLinks = getFilteredPages(pageContent)

        outboundLinks.foreach { outLink: String =>
          {
            visited.contains(outLink) match {
              case true => {
                println("Already Visited")
              }
              case false => {
                if (!frontier.contains(outLink)) {
                  //println(outLink)
                  frontier.enqueue(outLink)
                }
              }
            }
          }
        }
        pageCount += 1
        writePageToDb(pageCount, link, pageContent, outboundLinks.toList);
      } catch {
        case e: Exception => println("Ignored Request: " + e.getMessage)
      }
    }
  }
}
