package crawler

import com.netaporter.uri.Uri
import java.net.URI
import com.netaporter.uri.Uri.parse
import org.jsoup.{nodes, Jsoup}
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import org.mongodb.scala._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.collection.mutable
import collection.JavaConversions._

class Crawler(baseUrl: String,
              domain: String,
              startPage: String,
              ignoreList: List[String],
              ignoreUrlWithList: List[String],
              db: MongoDatabase,
              collectionName: String,
              encoding: String,
              ignoreParams: Seq[String]) {

  var visited = mutable.Set[String]()
  val frontier = new mutable.Queue[String]

  def getLinks(html: String): List[String] = {
    val doc: nodes.Document = Jsoup.parse(html)
    doc.setBaseUri(baseUrl)
    val links: Elements = doc.select("a[href]")

    links.map { link: Element =>
      {
        try {
          var ext = link.attr("href")

          if (!URI.create(ext).isAbsolute) {
            ext = baseUrl + ext
          }
          var uri: Uri = parse(ext)
          uri = uri.removeParams(ignoreParams)
          ext = uri.toString.replaceAll("""#(.+)""", "").replace("#", "")
          if (toRelative(ext) == "/")
            baseUrl + "/"
          else
            ext
        } catch {
          case e: Exception => {
              println("Bad URL: " + e.getMessage)
              ""
            }
        }
      }
    }.filter(_.nonEmpty).toList.distinct
  }

  def getHttp(url: String) = {
    try {
      val in = Source.fromURL(url, encoding)
      val response = in.getLines.mkString
      in.close()
      response
    } catch {
      case e: Exception =>
        throw new Exception("InvalidRequest: " + e.getMessage)
    }
  }

  def writePageToDb(pageCount: Int,
                    url: String,
                    pageContent: String,
                    outboundLinks: List[String]) = Future {
    val collection: MongoCollection[Document] =
      db.getCollection(collectionName)

    val document: Document = Document(
        "_id" -> pageCount,
        "url" -> toRelative(url),
        "content" -> pageContent,
        "outbound" -> outboundLinks.map(entry => toRelative(entry)).distinct
    )

    val insertObservable: Observable[Completed] =
      collection.insertOne(document)

    insertObservable.subscribe(
        new Observer[Completed] {
      override def onNext(result: Completed): Unit =
        println(s"onNext: $result")

      override def onError(e: Throwable): Unit = println(s"onError: $e")

      override def onComplete(): Unit = println("onComplete")
    })
  }

  def toRelative(url: String): String = {

    val relative = url.replace(baseUrl, "")

    if (relative.isEmpty || relative == "/")
      "/"
    else
      relative
  }

  def getFilteredPages(pageContent: String): Set[String] = {
    val outboundLinks = getLinks(pageContent)
    val outboundLinksFilterExtensions =
      outboundLinks.filter(x => !ignoreList.exists(x.endsWith))

    val outboundLinksFilterUrlParts = outboundLinksFilterExtensions.filterNot {
      url =>
        {
          ignoreUrlWithList.exists(url.contains)
        }
    }

    val outboundLinksFilterDomain =
      outboundLinksFilterUrlParts.filter(x => x.contains(domain))
    outboundLinksFilterDomain.toSet
  }

  def start = {
    var pageCount = 0

    val pageContent = getHttp(baseUrl + startPage)
    val outboundLinks = getFilteredPages(pageContent)
    frontier ++= outboundLinks

    writePageToDb(
        pageCount, baseUrl + startPage, pageContent, outboundLinks.toList)

    while (frontier.nonEmpty) {

      println(frontier.size)

      val link: String = frontier.dequeue()
      visited ++= Set[String](link)
      try {
        val pageContent = getHttp(link)
        val outboundLinks = getFilteredPages(pageContent)
        outboundLinks.foreach { outLink: String =>
          {
            if (!visited.contains(outLink) && !frontier.contains(outLink))
              frontier.enqueue(outLink)
          }
        }
        pageCount += 1
        writePageToDb(pageCount, link, pageContent, outboundLinks.toList)
      } catch {
        case e: Exception => println("Ignored Request: " + e.getMessage)
      }
    }
  }
}
