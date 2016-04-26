package logfile

import com.typesafe.config.Config
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.mongodb.scala.{ Completed, Observable, Observer, Document => _, _ }
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Projections._

import scala.collection.JavaConversions._
import database.Helpers._

import scala.collection.mutable
import scala.concurrent.Future

class Clustering(configFile: Config, db: MongoDatabase, collectionName: String) {

  val profilesHash = new scala.collection.mutable.HashMap[String, scala.collection.mutable.MutableList[Double]]()

  private def loadPreferences(prefs: BsonDocument) = {
    profilesHash.foreach {
      case (k, vs) =>
        val pref = prefs.get(k)
        if (pref == null) {
          vs += 0
        } else {
          vs += pref.asDouble().getValue
        }
    }
  }

  def loadData = {
    /*
    val ids = mutable.MutableList(String)
    val keys = db.getCollection(collectionName).find().projection(fields(include("preferences"))).results().flatMap(p => {
        ids += p.get[BsonString]("_id").get.getValue
        p.get[BsonDocument]("preferences").get.keySet().asScala
    }).toSet

    keys.map { k =>
      {
        profilesHash += (k -> mutable.MutableList[Double]())
      }
    }

    val promise = Promise[Seq[Document]]()

    db.getCollection(collectionName).find().collect().subscribe(
      (results: Seq[Document]) => {
        results.foreach { prof =>
          {
            val preferences = prof.get[BsonDocument]("preferences")
            if (preferences.isDefined)
              loadPreferences(preferences.get)
          }
        }
        promise.success(results)
      }, (t: Throwable) => promise.failure(t)
    )
    val f = promise.future

    Await.result(f, 3000 seconds)

    promise completeWith f
    promise.future onSuccess {
      case x => println(profilesHash("Vestuário Medicinal"))
    }


    for( i <- ids.indices){
      profilesHash.map {
        case (k, vs) =>
      }
    }*/

    val a: Seq[Map[(String, String), Double]] = db.getCollection(collectionName)
      .find()
      .projection(include("preferences", "pageTypes"))
      .results().map { doc =>
        {
          doc.get[BsonDocument]("preferences")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("preferences", m._1.replace(",", "")) -> m._2)
            .toMap ++
            doc.get[BsonDocument]("pageTypes")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("pageTypes", m._1) -> m._2)
            .toMap
        }
      }

    val keys: Set[(String, String)] = a.flatMap(m => m.keys).toSet

    val b: Seq[Map[(String, String), Double]] = a.map(m => keys.map(t => t -> 0.0).toMap ++ m)

    val res: Seq[List[Double]] = b.map(x => x.values.toList)

    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    val parsedData: RDD[Vector] = sc.parallelize(res.map(s => Vectors.dense(s.toArray))).cache()

    val numClusters = configFile.getInt("kugsha.profiles.numberOfClusters")
    val numOfIterations = configFile.getInt("kugsha.profiles.maxIterations")
    val clusters = KMeans.train(parsedData, numClusters, numOfIterations)

    store(keys, clusters.clusterCenters)

    sc.stop()

  }

  def store(header: Set[(String, String)], data: Array[Vector]) = {

    val keys = header.toList

    data.foreach { arr =>
      val preferences = mutable.HashMap[String, Double]()
      val pageTypes = mutable.HashMap[String, Double]()
      val currentArray = arr.toArray.toList
      for (x <- 1 until keys.size) {
        if (keys.get(x)._1 == "preferences")
          preferences += keys.get(x)._2 -> currentArray.get(x)
        else
          pageTypes += keys.get(x)._2 -> currentArray.get(x)
      }
      val document: Document = Document("preferences" -> preferences.toList, "pageTypes" -> pageTypes.toList)
      db.getCollection("kkprofilesproto").insertOne(document).headResult()
    }

  }

}
