package database

import org.mongodb.scala._


class Database (host: String, port: Int) {

  val connString = "mongodb://" + host + ":" + port

  def getDb(dbname: String) = {
    val client: MongoClient = MongoClient(connString)
    val database: MongoDatabase = client.getDatabase(dbname)
    database
  }

}
