package logfile

import com.typesafe.config.Config
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.mongodb.scala.bson._
import org.mongodb.scala.{ Document, MongoDatabase }
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.collection._
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Promise }
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import database.Helpers._

class Clustering(configFile: Config, db: MongoDatabase, collectionName: String) {

  val profilesHash = new mutable.HashMap[String, ListBuffer[Double]]()

  private def loadPreferences(prefs: BsonDocument, keys: Set[String]) = {
    keys.map { k =>
      {
        if (prefs.getDouble(k) == null) {
          profilesHash.get(k) match {
            case Some(savedP) => savedP += 0
            case None => profilesHash += k -> ListBuffer(0)
          }
        } else {
          profilesHash.get(k) match {
            case Some(savedP) => savedP += prefs.getDouble(k).getValue
            case None => profilesHash += k -> ListBuffer(prefs.getDouble(k).getValue)
          }
        }
      }
    }
  }

  def loadData = {

    val keys = db.getCollection(collectionName).find().projection(fields(include("preferences"), excludeId())).results().flatMap(p => p.get[BsonDocument]("preferences").get.keySet().asScala).toSet

    val promise = Promise[Seq[Document]]()

    db.getCollection(collectionName).find().collect().subscribe(
      (results: Seq[Document]) => {
        results.foreach { prof =>
          {
            val preferences = prof.get[BsonDocument]("preferences")
            if (preferences.isDefined)
              loadPreferences(preferences.get, keys)
          }
        }
        promise.success(results)
      }, (t: Throwable) => promise.failure(t)
    )
    val f = promise.future

    Await.result(f, 1000 seconds)

    promise completeWith f
    promise.future onSuccess {
      case x => println(profilesHash.size)
    }

    //val data = sc.textFile("mtcars.csv")
    //val parsedData = data.map(s => Vectors.dense(s.split(',').drop(1).map(_.toDouble))).cache()
  }

  def run = {
    //val numClusters = 2 // Value of K in Kmeans
    //val clusters = KMeans.train(parsedData, numClusters, 20)
  }
}
