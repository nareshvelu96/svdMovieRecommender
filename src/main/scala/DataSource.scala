package org.template

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        val title: String = properties.get[String]("title")
        val directors: Array[String] = properties.get[Array[String]]("directors")
        val writers: Array[String] = properties.get[Array[String]]("writers")
        val genres: Array[String] = properties.get[Array[String]]("genres")
        val actors: Array[String] = properties.get[Array[String]]("actors")
        val year: Int = properties.get[String]("year").toInt
        val language: String = properties.get[String]("language")
        val release_date: String = properties.get[String]("release_date")
        val ratings: Double = properties.get[Double]("ratings")
        val movieratings: Double = properties.get[Double]("movieratings")	
        val musicratings: Double = properties.get[Double]("musicratings")
        val gross: Double = properties.get[String]("gross").toDouble
        val budget: Double = properties.get[String]("budget").toDouble
        val score: Double = properties.get[Int]("score").toDouble
        
        

        Item(entityId, title, year, genres, writers, directors,
          actors, language, release_date, ratings, movieratings, musicratings, gross, budget, score)
      } catch {
        case e: Exception => {
          //logger.error(properties.get[Array[String]]("writer"))	
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }.cache()

    new TrainingData(items = itemsRDD)
  }
}

case class Item(item: String, title: String, year: Int,
                    genres: Array[String], writers: Array[String], directors:
                    Array[String], actors: Array[String], language: String, release_date: String,
                    ratings: Double, movieratings: Double, musicratings: Double, gross: Double, budget: Double, score: Double)

class TrainingData(val items: RDD[(String, Item)]) extends Serializable {
  override def toString = {
    s"items: [${items.count()}] (${items.take(3).toList}...)"
  }
}