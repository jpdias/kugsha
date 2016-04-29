package logfile

import com.typesafe.config.Config
import database.Helpers._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.{ Document => _, _ }

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class EntryProfile(id: String, avgTime: Double, visitCount: Int, rawData: Map[(String, String), Double])

class Clustering(configFile: Config, db: MongoDatabase, collectionName: String) {

  val profilesHash = new scala.collection.mutable.HashMap[String, scala.collection.mutable.MutableList[Double]]()

  def loadData() = {

    val collectProfiles: Seq[EntryProfile] = db.getCollection(collectionName)
      .find(Filters.gt("averageSessionTime", 0.0))
      .limit(25000)
      .projection(include("_id", "averageSessionTime", "totalPageViews", "preferences", "pageTypes"))
      .results().map { doc =>
        {
          val rawData = doc.get[BsonDocument]("preferences")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("preferences", m._1) -> m._2)
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

    clusteringStep(res, keys, collectProfiles)

  }

  def clusteringStep(res: Seq[List[Double]], keys: Set[(String, String)], profilesData: Seq[EntryProfile]) = {
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

    profilesData.foreach { pf =>
      val addZerosOpRes: Map[(String, String), Double] = keys.map(t => t -> 0.0).toMap ++ pf.rawData
      val res: List[Double] = addZerosOpRes.values.toList
      val pos = clusters.predict(Vectors.dense(res.toArray))
      result.get(pos)._1 += pf.avgTime
      result.get(pos)._2 += pf.visitCount
    }

    val usersPerCluster = result.map(x => x._1.size).toList

    val additionalFields: List[(Double, Double)] = result.map(x => (x._1.sum / x._1.length, (x._2.sum + 0.0) / x._2.length)).toList

    store(keys, clusters.clusterCenters, additionalFields, usersPerCluster)

    sc.stop()
  }

  def store(header: Set[(String, String)], data: Array[Vector], additionalFields: List[(Double, Double)], usersPerCluster: List[Int]) = {

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

      val document: Document = Document("preferences" -> preferences.toList, "pageTypes" -> pageTypes.toList, "averageSessionTime" -> additionalFields.get(i)._1, "averageVisitedPages" -> additionalFields.get(i)._2, "usersCount" -> usersPerCluster.get(i))
      db.getCollection(collectionPrototypes).insertOne(document).headResult()
      i += 1
    }

  }

}
