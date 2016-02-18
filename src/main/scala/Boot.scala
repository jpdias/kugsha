import crawler.Crawler
import database.Database

object Boot {


  val host = "localhost"
  val port = 27017
  val dbname = "kugsha"
  val collectionName = "silvajuliao-pages"

  val db =  new Database(host,port).getDb(dbname)

  val protocol = "http://"
  val domain = "silvajuliao.pt"
  val startPage = "/"
  val linkRegex = """href\s*=\s*\"*[^\">]*"""
  val ignoreList = List(".css",".js",".jpg",".jpeg",".png",".mp4",".woff",".ttf",".eot",".mp3",".pdf",".gif",".svg",".webp")
  val ignoreUrlWithList = List("mailto")
  val encoding = "utf-8"

  def main(args: Array[String]) {
    val crawler = new Crawler(protocol+domain, domain, startPage, linkRegex, ignoreList, ignoreUrlWithList, db, collectionName, encoding)
    crawler.start()
  }

}