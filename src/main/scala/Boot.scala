import crawler.Crawler
import database.Database
import com.typesafe.config.ConfigFactory
import collection.JavaConversions._

object Boot {

  val configFile = ConfigFactory.load()
  val host = configFile.getString("kugsha.database.host")
  val port = configFile.getInt("kugsha.database.port")
  val dbname = configFile.getString("kugsha.database.dbname")
  val collectionName = configFile.getString("kugsha.database.collectionName")

  val db =  new Database(host,port).getDb(dbname)

  val protocol = configFile.getString("kugsha.crawler.protocol")
  val domain = configFile.getString("kugsha.crawler.domain")
  val startPage = configFile.getString("kugsha.crawler.startPage")
  val linkRegex = configFile.getString("kugsha.crawler.linkRegex")
  val ignoreList = configFile.getStringList("kugsha.crawler.ignoreList").toList
  val ignoreUrlWithList = configFile.getStringList("kugsha.crawler.ignoreUrlWithList").toList
  val encoding = configFile.getString("kugsha.crawler.encoding")

  def main(args: Array[String]) {
    val crawler = new Crawler(protocol+domain, domain, startPage, linkRegex, ignoreList, ignoreUrlWithList, db, collectionName, encoding)
    crawler.start()
  }

}