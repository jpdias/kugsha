package crawler

import java.net.URL

import org.mongodb.scala._
import scala.io.Source
import scala.collection.mutable

class Crawler(domain: String, startPage: String = "/", linkRegexString: String, ignoreList: List[String],db: MongoDatabase) {

  val linkRegex = linkRegexString.r

  var visited = List[String]()
  val frontier = new mutable.Queue[String]

  def getLinks(html: String): List[String] =
    linkRegex.findAllMatchIn(html).map(_.toString.replaceAll("""href\s*=\s*\"*""","").stripPrefix("'").stripPrefix("\"").stripSuffix("'").stripSuffix("\"")).toList

  def getHttp(url: String) = {
    url.contains(domain) match {
      case true => {
        try {
          val link: URL = new URL(url)
          val in = Source.fromURL(link, "UTF-8")
          val response = in.getLines.mkString("\n")
          in.close()
          response
        } catch {
          case e: Exception => throw new Exception("InvalidRequest")
        }
      }
      case false => {
        try {
          val link: URL = new URL(domain + url)

          val in = Source.fromURL(link, "UTF-8")
          val response = in.getLines.mkString("\n")
          in.close()
          response
        } catch {
          case e: Exception => throw new Exception("InvalidRequest")
        }
      }
    }
  }

  def writePageToDb(pageCount: Int, url: String, pageContent: String, outboundLinks :List[String]) = {
    val collection: MongoCollection[Document] = db.getCollection("pages")
    val document: Document = Document("_id" -> pageCount, "url" -> url, "content" -> pageContent, "outbound" -> outboundLinks)
    val insertObservable: Observable[Completed] = collection.insertOne(document)

    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"onNext: $result")
      override def onError(e: Throwable): Unit = println(s"onError: $e")
      override def onComplete(): Unit = println("onComplete")
    })

  }

  def start() = {
    var pageCount = 0

    val pageContent = getHttp(startPage)
    val outboundLinks = getLinks(pageContent)
    val outboundLinksFilter = outboundLinks.filter(x => !ignoreList.exists(x.endsWith))
    frontier ++= outboundLinksFilter.toSet

    writePageToDb(pageCount, startPage, pageContent, outboundLinksFilter)

    while (frontier.nonEmpty) {
      pageCount += 1
      val link: String = frontier.dequeue()
      visited ++= List[String](link)
      try {
        val pageContent = getHttp(link)
        val outboundLinks = getLinks(pageContent)

        val outboundLinksFiltered = outboundLinks.filter(x => !ignoreList.exists(x.endsWith))

        outboundLinksFiltered.foreach { outLink: String =>
          {
            visited.contains(outLink) match {
              case true => {
                println("Already Visited")
              }
              case false => {
                outLink.contains(domain) match {
                  case false => println("Outside Page")
                  case true => {
                    if(!frontier.contains(outLink)){
                      println(outLink)
                      frontier.enqueue(outLink)
                      println(frontier.size)
                    }
                  }
                }
              }
            }
          }
        }
        writePageToDb(pageCount, link, pageContent, outboundLinksFiltered);
      } catch {
        case e: Exception => println("Ignored Request")
      }
    }
  }
}
