package logfile

import com.typesafe.config.Config
import database.Helpers._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.mongodb.scala.bson._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.{Document => _, _}

import scala.collection.JavaConversions._
import scala.collection.{Set, mutable}
import scala.collection.mutable.ListBuffer

case class EntryProfile(
    id: String,
    avgTime: Double,
    visitCount: Int,
    rawData: Map[(String, String), Double],
    resume: mutable.HashMap[String, Double],
    pageTypes: mutable.HashMap[String, Double],
    sessionData: SessionDetail
)

class Clustering(configFile: Config, db: MongoDatabase, collectionName: String) {

  val profilesHash = new scala.collection.mutable.HashMap[
      String, scala.collection.mutable.MutableList[Double]]()

  def loadData(clusterSessions: Boolean, clusterPreferences: Boolean) = {

    val catCollName =
      configFile.getString("kugsha.classification.categories.collectionName")

    val collectProfiles: Seq[EntryProfile] = db
      .getCollection(collectionName)
      .aggregate(List(sample(50000),
                      project(include("_id",
                                      "averageSessionTime",
                                      "totalPageViews",
                                      "preferences",
                                      "pageTypes",
                                      "sessionResume"))))
      .results()
      .map { doc =>
        {
          val rawData: Map[(String, String), Double] =
            doc
              .get[BsonDocument]("preferences")
              .getOrElse(BsonDocument())
              .mapValues(value => value.asDouble().getValue)
              .map(m => ("preferences", m._1) -> m._2)
              .toMap ++
            doc
              .get[BsonDocument]("pageTypes")
              .getOrElse(BsonDocument())
              .mapValues(value => value.asDouble().getValue)
              .map(m => ("pageTypes", m._1) -> m._2)
              .toMap

          val profileResume = new mutable.HashMap[String, Double]()
          val pageTypesOnly = new mutable.HashMap[String, Double]()

          rawData.keySet.foreach { entry =>
            {
              if (entry._1 == "preferences") {
                db.getCollection(catCollName)
                  .find(Filters.in("children", entry._2))
                  .results()
                  .headOption match {
                  case Some(document) => {
                      val catFound = document
                        .get[BsonArray]("parent")
                        .get
                        .getValues
                        .map(_.asString().getValue)
                        .mkString
                      profileResume.get(catFound) match {
                        case Some(en) =>
                          profileResume.update(
                              catFound, en + rawData.getOrElse(entry, 0.0))
                        case None =>
                          profileResume +=
                            catFound -> rawData.getOrElse(entry, 0.0)
                      }
                    }
                  case None =>
                }
              } else if (entry._1 == "pageTypes") {
                pageTypesOnly += entry._2 -> rawData.getOrElse(entry, 0.0)
              }
            }
          }
          val sessionResume = doc.get[BsonDocument]("sessionResume").get

          val sessionData = SessionDetail(
              innerInfo(
                  sessionResume
                    .get("sessionLength")
                    .asDocument()
                    .getInt32("level")
                    .getValue,
                  sessionResume
                    .get("sessionLength")
                    .asDocument()
                    .getNumber("level")
                    .doubleValue
              ),
              innerInfo(
                  sessionResume
                    .get("sessionDuration")
                    .asDocument()
                    .getInt32("level")
                    .getValue,
                  sessionResume
                    .get("sessionDuration")
                    .asDocument()
                    .getNumber("level")
                    .doubleValue
              ),
              innerInfo(
                  sessionResume
                    .get("meanTimePerPage")
                    .asDocument()
                    .getInt32("level")
                    .getValue,
                  sessionResume
                    .get("meanTimePerPage")
                    .asDocument()
                    .getNumber("level")
                    .doubleValue
              )
          )

          EntryProfile(doc.get[BsonString]("_id").get.getValue,
                       doc.get[BsonDouble]("averageSessionTime").get.getValue,
                       doc.get[BsonInt32]("totalPageViews").get.getValue,
                       rawData,
                       profileResume,
                       pageTypesOnly,
                       sessionData)
        }
      }

    val keys: Set[String] = collectProfiles.flatMap(m => m.resume.keys).toSet

    val addZerosOpRes: Seq[Map[String, Double]] =
      collectProfiles.map(m => keys.map(t => t -> 0.0).toMap ++ m.resume)

    val res: Seq[List[Double]] = addZerosOpRes.map(x => x.values.toList)

    if (clusterPreferences)
      clusteringStepPreferences(res, keys, collectProfiles)
    if (clusterSessions)
      clusteringStepSessions(res, keys, collectProfiles)
  }

  def clusteringStepPreferences(res: Seq[List[Double]],
                                keys: Set[String],
                                profilesData: Seq[EntryProfile]) = {
    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    val parsedData: RDD[Vector] =
      sc.parallelize(res.map(s => Vectors.dense(s.toArray))).cache()

    val numClusters = configFile.getInt("kugsha.profiles.numberOfClusters")
    val numOfIterations = configFile.getInt("kugsha.profiles.maxIterations")
    val clusters = KMeans.train(parsedData, numClusters, numOfIterations)

    val result = ListBuffer.fill(clusters.k)(
        (ListBuffer[Double](),
         ListBuffer[Int](),
         mutable.HashMap[String, Double](),
         mutable.HashMap[String, (Double, Double)]()))

    profilesData.foreach { pf =>
      val addZerosOpRes: Map[String, Double] =
        keys.map(t => t -> 0.0).toMap ++ pf.resume
      val res: List[Double] = addZerosOpRes.values.toList
      val pos = clusters.predict(Vectors.dense(res.toArray))
      result.get(pos)._1 += pf.avgTime
      result.get(pos)._2 += pf.visitCount
      result.get(pos)._3 ++= (for ((k, v) <- pf.pageTypes) yield
            k -> (v + result.get(pos)._3.getOrElse(k, 0.0)))
      result.get(pos)._4 += ("meanTimePerPage" ->
          (result.get(pos)._4.getOrElse("meanTimePerPage", (0.0, 0.0))._1 +
              pf.sessionData.meanTimePerPage.cat,
              result.get(pos)._4.getOrElse("meanTimePerPage", (0.0, 0.0))._1 +
              pf.sessionData.meanTimePerPage.value))
      result.get(pos)._4 += ("sessionLength" -> (result
                .get(pos)
                ._4
                .getOrElse("sessionLength", (0.0, 0.0))
                ._1 + pf.sessionData.sessionLength.cat,
              result.get(pos)._4.getOrElse("sessionLength", (0.0, 0.0))._1 +
              pf.sessionData.sessionLength.value))
      result.get(pos)._4 += ("sessionDuration" -> (result
                .get(pos)
                ._4
                .getOrElse("sessionDuration", (0.0, 0.0))
                ._1 + pf.sessionData.sessionDuration.cat,
              result.get(pos)._4.getOrElse("sessionDuration", (0.0, 0.0))._1 +
              pf.sessionData.sessionDuration.value))
    }

    val usersPerCluster = result.map(x => x._1.size).toList

    val additionalFields: List[(Double, Double)] = result
      .map(x => (x._1.sum / x._1.length, (x._2.sum + 0.0) / x._2.length))
      .toList

    val pageTypesAvg =
      ListBuffer.fill(clusters.k)(mutable.HashMap[String, Double]())

    val sessionDataAvg: ListBuffer[mutable.HashMap[String, (Int, Double)]] =
      ListBuffer.fill(clusters.k)(mutable.HashMap[String, (Int, Double)]())

    var ind = 0
    result.foreach(
        x => {
      x._3.keySet.map(k =>
            pageTypesAvg.get(ind) += k -> x._3.get(k).get / x._1.size)
      x._4.keySet.map(k =>
            sessionDataAvg.get(ind) +=
              k -> ((x._4.get(k).get._1 / x._1.size).toInt, x._4.get(k).get._1 / x._1.size))
      ind += 1
    })

    storeWithPrefs(keys,
                   clusters.clusterCenters,
                   additionalFields,
                   usersPerCluster,
                   pageTypesAvg,
                   sessionDataAvg)

    sc.stop()
  }

  def clusteringStepSessions(res: Seq[List[Double]],
                             keys: Set[String],
                             profilesData: Seq[EntryProfile]) = {
    val conf = new SparkConf()
      .setAppName("Clustering")
      .setMaster("local[1]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)

    val parsedData: RDD[Vector] = sc
      .parallelize(profilesData.map(s =>
                Vectors.dense(s.sessionData.sessionDuration.cat,
                              s.sessionData.meanTimePerPage.cat,
                              s.sessionData.sessionLength.cat)))
      .cache()

    val numClusters = configFile.getInt("kugsha.profiles.numberOfClusters")
    val numOfIterations = configFile.getInt("kugsha.profiles.maxIterations")
    val clusters = KMeans.train(parsedData, numClusters, numOfIterations)

    val result = ListBuffer.fill(clusters.k)(
        (ListBuffer[Double](),
         ListBuffer[Int](),
         mutable.HashMap[String, Double](),
         mutable.HashMap[String, Double]()))

    profilesData.foreach { pf =>
      val addZerosOpRes: Map[String, Double] =
        keys.map(t => t -> 0.0).toMap ++ pf.resume
      val res: List[Double] = addZerosOpRes.values.toList
      val pos = clusters.predict(
          Vectors.dense(pf.sessionData.sessionDuration.cat,
                        pf.sessionData.meanTimePerPage.cat,
                        pf.sessionData.sessionLength.cat))
      result.get(pos)._1 += pf.avgTime
      result.get(pos)._2 += pf.visitCount
      result.get(pos)._3 ++= (for ((k, v) <- pf.pageTypes) yield
            k -> (v + result.get(pos)._3.getOrElse(k, 0.0)))
      result.get(pos)._4 ++= (for ((k, v) <- pf.resume) yield
            k -> (v + result.get(pos)._4.getOrElse(k, 0.0)))
    }

    val usersPerCluster = result.map(x => x._1.size).toList

    val additionalFields: List[(Double, Double)] = result
      .map(x => (x._1.sum / x._1.length, (x._2.sum + 0.0) / x._2.length))
      .toList

    val pageTypesAvg =
      ListBuffer.fill(clusters.k)(mutable.HashMap[String, Double]())

    val prefsDataAvg: ListBuffer[mutable.HashMap[String, Double]] =
      ListBuffer.fill(clusters.k)(mutable.HashMap[String, Double]())

    var ind = 0
    result.foreach(
        x => {
      x._3.keySet.map(k =>
            pageTypesAvg.get(ind) += k -> x._3.get(k).get / x._1.size)
      x._4.keySet.map(k =>
            prefsDataAvg.get(ind) += k -> x._4.get(k).get / x._1.size)
      ind += 1
    })

    storeWithSessions(keys,
                      clusters.clusterCenters,
                      additionalFields,
                      usersPerCluster,
                      pageTypesAvg,
                      prefsDataAvg)

    sc.stop()
  }

  def storeWithSessions(
      header: Set[String],
      sessionClusters: Array[Vector],
      additionalFields: List[(Double, Double)],
      usersPerCluster: List[Int],
      pageTypesAvg: ListBuffer[mutable.HashMap[String, Double]],
      prefsDataAvg: ListBuffer[mutable.HashMap[String, Double]]) = {

    val collectionPrototypes =
      configFile.getString("kugsha.profiles.collectionPrototypesSessions")

    val keys = List("sessionDuration", "meanTimePerPage", "sessionLength")

    var i = 0
    sessionClusters.foreach { arr =>
      if (usersPerCluster.get(i) > configFile.getInt(
              "kugsha.profiles.usersPerClusterMinThreshold")) {
        val sessions = mutable.HashMap[String, Int]()
        println(arr.toArray.toList)
        val currentArray = arr.toArray.toList
        for (x <- keys.indices) {
          sessions += keys.get(x) -> currentArray.get(x).toInt
        }

        val prefsData = prefsDataAvg
          .get(i)
          .keySet
          .map(
              entry => entry -> prefsDataAvg.get(i).getOrElse(entry, 0.0)
          )

        val document: Document = Document(
            "sessionData" -> sessions.toList,
            "pageTypes" -> pageTypesAvg.get(i).toList,
            "averageSessionTime" -> additionalFields.get(i)._1,
            "averageVisitedPages" -> additionalFields.get(i)._2,
            "averageTimePerPage" -> (additionalFields.get(i)._1 / additionalFields
                  .get(i)
                  ._2),
            "usersCount" -> usersPerCluster.get(i),
            "preferences" -> prefsData.toList
        )

        db.getCollection(collectionPrototypes).insertOne(document).headResult()
        i += 1
      } else {
        i += 1
      }
    }
  }

  def storeWithPrefs(
      header: Set[String],
      data: Array[Vector],
      additionalFields: List[(Double, Double)],
      usersPerCluster: List[Int],
      pageTypesAvg: ListBuffer[mutable.HashMap[String, Double]],
      sessionDataAvg: ListBuffer[mutable.HashMap[String, (Int, Double)]]) = {

    val collectionPrototypes =
      configFile.getString("kugsha.profiles.collectionPrototypesPrefs")

    val keys = header.toList

    var i = 0
    data.foreach { arr =>
      if (usersPerCluster.get(i) > configFile.getInt(
              "kugsha.profiles.usersPerClusterMinThreshold")) {
        val preferences = mutable.HashMap[String, Double]()
        val currentArray = arr.toArray.toList
        for (x <- 1 until keys.size) {
          preferences += keys.get(x) -> currentArray.get(x)
        }

        val sessionData = sessionDataAvg
          .get(i)
          .keySet
          .map(
              entry =>
                Document(
                    entry ->
                    Document(
                        "level" -> sessionDataAvg
                          .get(i)
                          .getOrElse(entry, (0, 0.0))
                          ._1,
                        "value" -> sessionDataAvg
                          .get(i)
                          .getOrElse(entry, (0, 0.0))
                          ._2
                    ))
          )

        val document: Document = Document(
            "preferences" -> preferences.toList,
            "pageTypes" -> pageTypesAvg.get(i).toList,
            "averageSessionTime" -> additionalFields.get(i)._1,
            "averageVisitedPages" -> additionalFields.get(i)._2,
            "averageTimePerPage" -> (additionalFields.get(i)._1 / additionalFields
                  .get(i)
                  ._2),
            "usersCount" -> usersPerCluster.get(i),
            "sessionResume" -> sessionData.toList
        )

        db.getCollection(collectionPrototypes).insertOne(document).headResult()

        i += 1
      }
    }
  }
}
