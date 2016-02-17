import crawler.Crawler
import database.Database

object Boot {


  val host = "localhost"
  val port = 27017
  val dbname = "kugsha"
  val collectionName = "clickfiel-pages"

  val db =  new Database(host,port).getDb(dbname)

  val protocol = "http://"
  val domain = "clickfiel.pt"
  val startPage = "/"
  val linkRegex = """href\s*=\s*\"*[^\">]*"""
  val ignoreList = List(".css",".js",".jpg",".jpeg",".png",".mp4",".woff",".ttf",".eot",".mp3",".pdf",".gif",".svg",".webp")
  val encoding = "iso-8859-1"

  def main(args: Array[String]) {
    val crawler = new Crawler(protocol+domain, domain, startPage, linkRegex, ignoreList, db, collectionName, encoding)
    crawler.start()
  }

}