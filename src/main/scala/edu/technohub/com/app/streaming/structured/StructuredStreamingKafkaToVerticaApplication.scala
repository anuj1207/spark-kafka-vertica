package edu.technohub.com.app.streaming.structured

import edu.technohub.com.models.Person
import edu.technohub.com.services.SparkService
import edu.technohub.com.sources.{KafkaSource, VerticaSource}
import org.apache.spark.sql.{DataFrame, SparkSession}

object StructuredStreamingKafkaToVerticaApplication extends SparkService(new KafkaSource, new VerticaSource) with App{

  val spark: SparkSession = createSparkSession()

  val df: DataFrame = createStreamingDataFrame(spark)

  val parsedDataFrame: DataFrame = df.transform(parseFromOneColumn(_, Person.schema)) //TODO::check::Use your model here

  val transformedDF: DataFrame = parsedDataFrame.transform(transformationLogic) //TODO::check::Add you logic inside `transformationLogic`

  customWriteStreamingDF(transformedDF).awaitTermination()

}
