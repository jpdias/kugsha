package logfile

import com.typesafe.config.Config
import database.Helpers._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.{Document => _, _}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class EntryProfile(id: String, avgTime: Double, visitCount: Int, rawData: Map[(String, String), Double])

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

  def loadData() = {
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



    for( i <- ids.indices){
      profilesHash.map {
        case (k, vs) =>
      }
    }*/

    val collectProfiles: Seq[EntryProfile] = db.getCollection(collectionName)
      .find()
      .projection(include("_id", "averageSessionTime", "totalPageViews", "preferences", "pageTypes"))
      .results().map { doc =>
        {
          val rawData = doc.get[BsonDocument]("preferences")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("preferences", m._1.replace(",", "")) -> m._2)
            .toMap ++
            doc.get[BsonDocument]("pageTypes")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("pageTypes", m._1) -> m._2)
            .toMap
          EntryProfile(doc.get[BsonString]("_id").get.getValue, doc.get[BsonDouble]("averageSessionTime").get.getValue, doc.get[BsonInt32]("totalPageViews").get.getValue, rawData)
        }
      }

    val keys: Set[(String, String)] = collectProfiles.flatMap(m => m.rawData.keys).toSet

    val addZerosOpRes: Seq[Map[(String, String), Double]] = collectProfiles.map(m => keys.map(t => t -> 0.0).toMap ++ m.rawData)

    val res: Seq[List[Double]] = addZerosOpRes.map(x => x.values.toList)

    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    val parsedData: RDD[Vector] = sc.parallelize(res.map(s => Vectors.dense(s.toArray))).cache()

    val numClusters = configFile.getInt("kugsha.profiles.numberOfClusters")
    val numOfIterations = configFile.getInt("kugsha.profiles.maxIterations")
    val clusters = KMeans.train(parsedData, numClusters, numOfIterations)

    val result: ListBuffer[(ListBuffer[Double], ListBuffer[Int])] = ListBuffer.fill(clusters.k)((ListBuffer[Double](), ListBuffer[Int]()))

    collectProfiles.foreach { pf =>
      val addZerosOpRes: Map[(String, String), Double] = keys.map(t => t -> 0.0).toMap ++ pf.rawData
      val res: List[Double] = addZerosOpRes.values.toList
      val pos = clusters.predict(Vectors.dense(res.toArray))
      result.get(pos)._1 += pf.avgTime
      result.get(pos)._2 += pf.visitCount
    }

    val additionalFields: List[(Double, Double)] = result.map(x => (x._1.sum / x._1.length, (x._2.sum + 0.0) / x._2.length)).toList
    store(keys, clusters.clusterCenters, additionalFields)

    sc.stop()

  }

  def store(header: Set[(String, String)], data: Array[Vector], additionalFields: List[(Double, Double)]) = {

    val collectionPrototypes = configFile.getString("kugsha.profiles.collectionPrototypes")

    val keys = header.toList

    var i = 0
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

      val document: Document = Document("preferences" -> preferences.toList, "pageTypes" -> pageTypes.toList, "averageSessionTime" -> additionalFields.get(i)._1, "averageVisitedPages" -> additionalFields.get(i)._2)
      db.getCollection(collectionPrototypes).insertOne(document).headResult()
      i += 1
    }

  }

}
