package logfile

import com.typesafe.config.Config
import database.Helpers._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.{ Document => _, _ }
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class EntryProfile(
  id:         String,
  avgTime:    Double,
  visitCount: Int,
  rawData:    Map[(String, String), Double],
  resume:     mutable.HashMap[String, Double],
  pageTypes:  mutable.HashMap[String, Double]
)

class Clustering(configFile: Config, db: MongoDatabase, collectionName: String) {

  val profilesHash = new scala.collection.mutable.HashMap[String, scala.collection.mutable.MutableList[Double]]()

  def loadData() = {

    val catCollName = configFile.getString("kugsha.classification.categories.collectionName")

    val collectProfiles: Seq[EntryProfile] = db.getCollection(collectionName)
      .aggregate(List(filter(
        Filters.gt("averageSessionTime", 0.0)
      ), sample(25000), project(include("_id", "averageSessionTime", "totalPageViews", "preferences", "pageTypes"))))
      .results().map { doc =>
        {
          val rawData: Map[(String, String), Double] = doc.get[BsonDocument]("preferences")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("preferences", m._1) -> m._2)
            .toMap ++
            doc.get[BsonDocument]("pageTypes")
            .getOrElse(BsonDocument())
            .mapValues(value => value.asDouble().getValue)
            .map(m => ("pageTypes", m._1) -> m._2)
            .toMap
          val profileResume = new mutable.HashMap[String, Double]()
          val pageTypesOnly = new mutable.HashMap[String, Double]()

          rawData.keySet.foreach { entry =>
            {
              if (entry._1 == "preferences") {
                db.getCollection(catCollName).find(Filters.in("children", entry._2)).results().headOption match {
                  case Some(document) => {
                    val catFound = document.get[BsonArray]("parent").get.getValues.map(_.asString().getValue).mkString
                    profileResume.get(catFound) match {
                      case Some(en) => profileResume.update(catFound, en + rawData.getOrElse(entry, 0.0))
                      case None => profileResume += catFound -> rawData.getOrElse(entry, 0.0)
                    }
                  }
                  case None =>
                }
              } else if (entry._1 == "pageTypes") {
                pageTypesOnly += entry._2 -> rawData.getOrElse(entry, 0.0)
              }
            }
          }
          EntryProfile(doc.get[BsonString]("_id").get.getValue, doc.get[BsonDouble]("averageSessionTime").get.getValue, doc.get[BsonInt32]("totalPageViews").get.getValue, rawData, profileResume, pageTypesOnly)
        }
      }

    val keys: Set[String] = collectProfiles.flatMap(m => m.resume.keys).toSet

    val addZerosOpRes: Seq[Map[String, Double]] = collectProfiles.map(m => keys.map(t => t -> 0.0).toMap ++ m.resume)

    val res: Seq[List[Double]] = addZerosOpRes.map(x => x.values.toList)

    clusteringStep(res, keys, collectProfiles)

  }

  def clusteringStep(res: Seq[List[Double]], keys: Set[String], profilesData: Seq[EntryProfile]) = {
    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    val parsedData: RDD[Vector] = sc.parallelize(res.map(s => Vectors.dense(s.toArray))).cache()

    val numClusters = configFile.getInt("kugsha.profiles.numberOfClusters")
    val numOfIterations = configFile.getInt("kugsha.profiles.maxIterations")
    val clusters = KMeans.train(parsedData, numClusters, numOfIterations)

    val result = ListBuffer.fill(clusters.k)((ListBuffer[Double](), ListBuffer[Int](), mutable.HashMap[String, Double]()))

    profilesData.foreach { pf =>

      val addZerosOpRes: Map[String, Double] = keys.map(t => t -> 0.0).toMap ++ pf.resume
      val res: List[Double] = addZerosOpRes.values.toList
      val pos = clusters.predict(Vectors.dense(res.toArray))
      result.get(pos)._1 += pf.avgTime
      result.get(pos)._2 += pf.visitCount
      result.get(pos)._3 ++= (for ((k, v) <- pf.pageTypes) yield k -> (v + result.get(pos)._3.getOrElse(k, 0.0)))
    }

    val usersPerCluster = result.map(x => x._1.size).toList

    val additionalFields: List[(Double, Double)] = result.map(x => (x._1.sum / x._1.length, (x._2.sum + 0.0) / x._2.length)).toList

    val pageTypesAvg = ListBuffer.fill(clusters.k)(mutable.HashMap[String, Double]())

    var ind = 0
    result.foreach(x => {
      x._3.keySet.map(k => pageTypesAvg.get(ind) += k -> x._3.get(k).get / x._1.size)
      ind += 1
    })

    store(keys, clusters.clusterCenters, additionalFields, usersPerCluster, pageTypesAvg)

    sc.stop()
  }

  def store(header: Set[String], data: Array[Vector], additionalFields: List[(Double, Double)], usersPerCluster: List[Int], pageTypesAvg: ListBuffer[mutable.HashMap[String, Double]]) = {

    val collectionPrototypes = configFile.getString("kugsha.profiles.collectionPrototypes")

    val keys = header.toList

    var i = 0
    data.foreach { arr =>
      val preferences = mutable.HashMap[String, Double]()
      val currentArray = arr.toArray.toList
      for (x <- 1 until keys.size) {
        preferences += keys.get(x) -> currentArray.get(x)
      }

      val document: Document = Document(
        "preferences" -> preferences.toList,
        "pageTypes" -> pageTypesAvg.get(i).toList,
        "averageSessionTime" -> additionalFields.get(i)._1,
        "averageVisitedPages" -> additionalFields.get(i)._2,
        "averageTimePerPage" -> (additionalFields.get(i)._1 / additionalFields.get(i)._2),
        "usersCount" -> usersPerCluster.get(i)
      )

      db.getCollection(collectionPrototypes).insertOne(document).headResult()
      i += 1
    }

  }

}
