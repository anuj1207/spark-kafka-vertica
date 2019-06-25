package edu.technohub.com.models

import edu.technohub.com.utils.Constants.{NAME, AGE}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class Person(name: String, age: Int)

object Person{

  /*Create schema for your model*/
  def schema: StructType =
    StructType(
      List(
        StructField(NAME, StringType, nullable = true),
        StructField(AGE, IntegerType, nullable = true)
      )
    )
}
