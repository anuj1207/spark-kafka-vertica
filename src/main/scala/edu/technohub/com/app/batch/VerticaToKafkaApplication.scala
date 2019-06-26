package edu.technohub.com.app.batch

import edu.technohub.com.models.Person
import edu.technohub.com.services.SparkService
import edu.technohub.com.sources.{KafkaSource, VerticaSource}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This Application is to read data from Vertica source and dump that data into kafka
  * Can add some logic here accordingly
  * */
object VerticaToKafkaApplication extends SparkService(new VerticaSource, new KafkaSource) with App{

  val spark: SparkSession = createSparkSession()

  val df: DataFrame = inbound.readFromSource(spark)

  private val fieldNames = Person.schema.fieldNames.toList

  val parsedDataFrame: DataFrame = df.transform(mergeIntoOneColumn(_, fieldNames, "value")) //Kafka needs a `value` field when dumping data

  val transformedDF: DataFrame = parsedDataFrame.transform(transformationLogic) //TODO::check::Add you logic inside `transformationLogic`

  outbound.saveToSource(transformedDF)

  spark.stop()

}
