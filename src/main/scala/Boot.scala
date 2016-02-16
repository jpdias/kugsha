import crawler.Crawler
import database.Database

object Boot {


  val host = "localhost"
  val port = 27017
  val dbname = "kugsha"

  val db =  new Database(host,port).getDb(dbname)

  val protocol = "http://"
  val domain = "silvajuliao.pt"
  val startPage = "/"
  val linkRegex = """href\s*=\s*\"*[^\">]*"""
  val ignoreList = List(".css",".js",".jpg",".jpeg",".png",".mp4",".woff",".ttf",".eot",".mp3",".pdf",".gif",".svg",".webp")

  def main(args: Array[String]) {
    val crawler = new Crawler(protocol+domain,startPage,linkRegex,ignoreList,db)
    crawler.start()
  }

}